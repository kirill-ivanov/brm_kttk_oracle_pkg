CREATE OR REPLACE PACKAGE PK10_BALANCE_EISUP_REP_GUI
IS
    --
    -- Пакет для поддержки импорта из ЕИСУП
    -- eisup_payment_t, eisup_pay_transfer_t
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK10_BALANCE_EISUP_REP_GUI';
    -- ==============================================================================
   
-- для логирования операций
     gv_app_user   L01_MESSAGES.APP_USER%TYPE;
     gv_message   varchar2(32000);

-- для логирования операций
    procedure log_init(
        p_OS_USER   L01_MESSAGES.OS_USER%TYPE,
        p_APP_USER   L01_MESSAGES.APP_USER %TYPE
    );



    type t_refc is ref cursor;

    -- справочник кодов ошибок EISUP_RP_BALANCE_CODE_DCT
    PROCEDURE CODE_DCT_list( 
        p_recordset  OUT t_refc, 
        p_CODE_GROUP_ID IN INTEGER
    );


    -- заголовок отчета
    procedure eisup_rp_header_list(
        p_recordset  OUT t_refc,
        p_REPORT_ID  in number,
        p_report_group in number default null
    )
    ;

--    procedure eisup_rp_header_list_by_type(
--        p_recordset  OUT t_refc,
--        p_REPORT_TYPE_ID  in number
--    )
--    ;
    
    function eisup_rp_header_add(
        p_id                in number, 
        p_rep_period        in number, 
        p_report_name       in varchar2, 
        p_user_name         in varchar2,
        p_report_type_id    in number, 
        p_rep_period_from   in number ,
        p_rep_period_to     in number,
        p_file_name_1       in varchar2 default null
    ) return number
    ;
    function eisup_rp_header_add_nostart(
        p_id                in number, 
        p_rep_period        in number, 
        p_report_name       in varchar2, 
        p_user_name         in varchar2,
        p_report_type_id    in number, 
        p_rep_period_from   in number ,
        p_rep_period_to     in number,
        p_file_name_1       in varchar2 default null
    ) return number
    ;
    procedure eisup_rp_header_start(
        p_id                in number
    )
    ;

    procedure eisup_rp_header_del(
        p_id                in number 
    );

   
--    procedure eisup_rp_header_start(
--        p_id                in number 
--    )
--    ;
    procedure start_job(
        p_report_id in number
    )
    ;
   

    -- список договоров из 1С   
    PROCEDURE EISUP_RP_BODY_FOUND_LIST( 
        p_recordset  OUT t_refc,
        p_REPORT_ID in number 
    )
    ;

    -- список EISUP_RP_BODY_NOTFOUND договоров из BRM   
    PROCEDURE EISUP_RP_BODY_NOTFOUND_LIST( 
        p_recordset  OUT t_refc,
        p_REPORT_ID in number 
    )
    ;

    -- детализации платежей в BRM
    PROCEDURE PAY_BRM_BY_CONTRACT_LIST( 
        p_recordset  OUT t_refc,
        p_CONTRACT_ID in number,
        p_period_ID in number
    )
    ;
    PROCEDURE PAY_BRM_BY_ACCOUNT_LIST( 
        p_recordset  OUT t_refc,
        p_ACCOUNT_ID in number,
        p_period_ID in number
    )
    ;


    -- детализации счетов в BRM
    PROCEDURE BILL_BRM_BY_CONTRACT_LIST( 
        p_recordset  OUT t_refc,
        p_CONTRACT_ID in number,
        p_period_ID in number
    )
    ;
    PROCEDURE BILL_BRM_BY_ACCOUNT_LIST( 
        p_recordset  OUT t_refc,
        p_ACCOUNT_ID in number,
        p_period_ID in number
    )
    ;

    -- детализации платежей в 1C
    PROCEDURE PAY_1C_BY_CONTRACT_LIST( 
        p_recordset  OUT t_refc,
        p_REPORT_ID   in number,
        p_CONTRACT_NO in varchar2
    )
    ;

    -- типы отчетов
    procedure REPORT_TYPE_list (
        p_recordset  OUT t_refc,
        p_report_group in number default null 
    )
    ;
    
    -- отчет, в котором за основу берутся данные 1С (Отчет по исходным данным из 1C)
    procedure report_type_1_start(
        p_id                in number 
    );


    --=====================================================================
    -- таблица связи договоров BRM с договорами 1С  (EISUP_RP_CONTR_LINK)
    --=====================================================================
    procedure BRM_1C_CONTR_LINK_list(
        p_recordset  OUT t_refc
    )
    ;
    function BRM_1C_CONTR_LINK_add(
        p_id                in number,
        p_BRM_CONTRACT_NO     in varchar2, 
        p_BRM_INN             in varchar2,
        p_TPI_CONTRACT_NO     in varchar2, 
        p_TPI_INN             in varchar2
    ) return number
    ;
    procedure BRM_1C_CONTR_LINK_update(
        p_id                in number,
        p_BRM_CONTRACT_NO     in varchar2, 
        p_BRM_INN             in varchar2,
        p_TPI_CONTRACT_NO     in varchar2, 
        p_TPI_INN             in varchar2
    )
    ; 
    procedure BRM_1C_CONTR_LINK_delete(
        p_id                in number
    )
    ; 

    --=====================================================================
    -- список договоров,лицевых счетов BRM
    --=====================================================================
    procedure BRM_CONTRACT_ACC_list(
        p_recordset  OUT t_refc
    )
    ;

    --=====================================================================
    -- список договоров,лицевых счетов 1C
    --=====================================================================
    procedure C1_CONTRACT_ACC_list(
        p_recordset  OUT t_refc
    )
    ;

    --=====================================================================
    -- Отчет по счетам BRM, выгруженным в 1С
    --=====================================================================
    procedure BRM_BILL_list(
        p_recordset  OUT t_refc,
        p_period_id  in varchar2  -- пример : 201609
    );

    --=====================================================================
    -- Отчет по привязке договоров BRM с одним лицевым к договорам в 1С  (EISUP_RP2)
    --=====================================================================
    procedure EISUP_RP2_list(
        p_recordset  OUT t_refc,
        p_REPORT_ID in number 
    );

    --=====================================================================
    -- Таблица связи договоров 1С-BRM  :    EISUP_RP_CONTRACT_T
    --=====================================================================
    procedure EISUP_RP_CONTRACT_T_list (
        p_recordset  OUT t_refc 
    )
    ;
    function EISUP_RP_CONTRACT_T_add(
          p_ID                   number,
          p_CONTRACT_ID          INTEGER,
          p_EISUP_CONTRACT_CODE  VARCHAR2,
          p_EISUP_CONTRACT_NO    VARCHAR2,
          p_EISUP_INN            VARCHAR2,
          p_EISUP_KPP            VARCHAR2,
          p_NOTES                VARCHAR2
    ) return number
    ;
    procedure EISUP_RP_CONTRACT_T_del(
        p_id                in number 
    )
    ;

    --=====================================================================
    -- Таблица связи лицевых 1С-BRM  :   EISUP_RP_CONTRACT_ACCOUNT_T
    --=====================================================================
    procedure EISUP_RP_CONTRACT_ACC_T_list (
        p_recordset  OUT t_refc 
    )
    ;
    function EISUP_RP_CONTRACT_ACC_T_add(
          p_ID                   number,
          p_CONTRACT_ID          INTEGER,
          p_ACCOUNT_ID           integer,
          p_EISUP_CONTRACT_CODE  VARCHAR2,
          p_EISUP_CONTRACT_NO    VARCHAR2,
          p_EISUP_INN            VARCHAR2,
          p_EISUP_KPP            VARCHAR2,
          p_NOTES                VARCHAR2
    ) return number
    ;
    procedure EISUP_RP_CONTRACT_ACC_T_del(
        p_id                in number 
    )
    ;


    --=====================================================================
    -- оборотно сальдовая ведомость
    --=====================================================================
    procedure oborot_sald_vedom(
         p_recordset  OUT t_refc,
         p_period_id in number 
    )
    ;

    --=====================================================================
    -- оборотно сальдовая ведомость по лицевым с перенесенными балансами
    --=====================================================================
    procedure oborot_sald_vedom_moved(
         p_recordset  OUT t_refc,
         p_period_id in number 
    )
    ;
    --=====================================================================
    -- детализации платежей в 1C для :
    -- оборотно сальдовая ведомость по лицевым с перенесенными балансами
    --=====================================================================
    PROCEDURE PAY_1C_for_OSV_moved( 
        p_recordset  OUT t_refc,
        --
        p_YYYYMM in number,
        p_contract_code in varchar2,
        p_eisup_inn in varchar2,
        p_eisup_kpp in varchar2
    )
    ;    
    

    --=====================================================================
    -- детализации платежей в BRM запериод (отчет по просьбе Макеева от 06.03.2017)
    --=====================================================================
    procedure report_pay_brm (
        p_recordset  OUT t_refc,
        --
        --p_period_id in number
        p_date_from in date,
        p_date_to in date
    )
    ;
    
END ;
/
CREATE OR REPLACE PACKAGE BODY PK10_BALANCE_EISUP_REP_GUI
IS

-- для логирования операций
    procedure log_init(
        p_OS_USER   L01_MESSAGES.OS_USER%TYPE,
        p_APP_USER   L01_MESSAGES.APP_USER %TYPE
    )
    is
    begin
        pk01_syslog.g_OS_USER := p_OS_USER;
        gv_app_user := p_APP_USER;  
    end;   



    procedure start_job(
        p_report_id in number
    )
    is
       job_id number;
       v_what varchar2(4000);
       v_errm varchar2(4000);
       v_pr EISUP_RP_HEADER%rowtype;
    begin
    
        begin
                select * into v_pr from EISUP_RP_HEADER where  REPORT_ID = p_report_id;
            exception when no_data_found then
                 raise_application_error(-20001,'Не найдено задание id='||p_report_id);
                 return;
        end;
    
        if (v_pr.REPORT_TYPE_ID = 1) then
            v_what := '
               begin
                    PK10_BALANCE_EISUP_REP_GUI.report_type_1_start('||p_report_id||');
                    commit;
               end;
               ';
        elsif (v_pr.REPORT_TYPE_ID = 2) then
            v_what := '
               begin
                    PK10_BALANCE_EISUP_REP2.report_type_2_start('||p_report_id||');
                    commit;
               end;
               ';
        elsif (v_pr.REPORT_TYPE_ID = 3) then
            v_what := '
               begin
                    PK10_BALANCE_EISUP_REP3.report_type_3_start('||p_report_id||');
                    commit;
               end;
               ';
        elsif (v_pr.REPORT_TYPE_ID = 4) then
            v_what := '
               begin
                    PK10_BALANCE_EISUP_REP4.report_type_4_start('||p_report_id||');
                    commit;
               end;
               ';
        elsif (v_pr.REPORT_TYPE_ID = 5) then
            v_what := '
               begin
                    PK10_BALANCE_EISUP_REP5.report_type_5_start('||p_report_id||');
                    commit;
               end;
               ';
        elsif (v_pr.REPORT_TYPE_ID = 6) then
            v_what := '
               begin
                    PK10_BALANCE_EISUP_REP6.report_type_6_start('||p_report_id||');
                    commit;
               end;
               ';
        else
                 raise_application_error(-20001,'тип отчета='||v_pr.REPORT_TYPE_ID||' не реализован, rep_id='||p_report_id);
        end if;

       dbms_job.submit(job_id,v_what);
       commit;
    
    Exception when others then
                 v_errm := replace(replace(SUBSTR(SQLERRM, 1, 3999),CHR(13),' '),CHR(10),'');
                 update EISUP_RP_HEADER rh
                    set 
                        RH.STATE = 'err',
                        STOP_TIME  = sysdate,
                        ERR_MESSAGE = v_errm
                 where RH.REPORT_ID = p_report_id;
                 commit;
    end;


-- ========================================================================== --
-- Работа с входящим балансом
-- ========================================================================== --

    -- справочник кодов ошибок EISUP_RP_BALANCE_CODE_DCT
    PROCEDURE CODE_DCT_list( 
        p_recordset  OUT t_refc, 
        p_CODE_GROUP_ID IN INTEGER
    )
    is 
    begin
        open p_recordset for
            select CODE, CODE_DESCR
            from EISUP_RP_BALANCE_CODE_DCT
            where  CODE_GROUP_ID = p_CODE_GROUP_ID
            order by CODE
        ; 
    end;
               
    -- заголовок отчета
    procedure eisup_rp_header_list(
        p_recordset  OUT t_refc,
        p_REPORT_ID  in number,
        p_report_group in number default null
    )
    is
    begin
        open p_recordset for 
            select REPORT_ID, REP_PERIOD, REPORT_NAME, USER_NAME, CREATE_TIME, START_TIME, STOP_TIME, STATE, DROP_REQUEST, ERR_MESSAGE, PARTITION_NAME,
            h.REPORT_TYPE_ID,
            REP_PERIOD_FROM, REP_PERIOD_TO,
            FILE_NAME_1
            from EISUP_RP_HEADER h, EISUP_RP_REPORT_TYPE t
            where 
              (p_REPORT_ID is null or p_REPORT_ID = REPORT_ID)
              and 
              T.REPORT_TYPE_ID = H.REPORT_TYPE_ID
              and
              (p_report_group is null or p_report_group = T.REPORT_GROUP_ID ) 
              and 
              nvl(DROP_REQUEST,'N') != 'Y'
            order by REPORT_ID   
        ;
        
        
    end;
    
--    procedure eisup_rp_header_list_by_type(
--        p_recordset  OUT t_refc,
--        p_REPORT_TYPE_ID  in number
--    )
--    is
--    begin
--        open p_recordset for 
--            select REPORT_ID, REP_PERIOD, REPORT_NAME, USER_NAME, CREATE_TIME, START_TIME, STOP_TIME, STATE, DROP_REQUEST, ERR_MESSAGE, PARTITION_NAME,
--            REPORT_TYPE_ID,
--            REP_PERIOD_FROM, REP_PERIOD_TO
--            from EISUP_RP_HEADER
--            where 
--              (p_REPORT_TYPE_ID is null or p_REPORT_TYPE_ID = REPORT_TYPE_ID)
--              and 
--              nvl(DROP_REQUEST,'N') != 'Y'
--            order by REPORT_ID   
--        ;
--        
--        
--    end;


    function eisup_rp_header_add(
        p_id                in number, 
        p_rep_period        in number, 
        p_report_name       in varchar2, 
        p_user_name         in varchar2,
        p_report_type_id    in number, 
        p_rep_period_from   in number ,
        p_rep_period_to     in number,
        p_file_name_1       in varchar2 default null
    ) return number
    is
        v_prcName    CONSTANT VARCHAR2(30) := 'eisup_rp_header_add';
        v_id            integer;
        p_create_time date;
    begin
        
        v_id := p_id;
        if (v_id is null) then select SQ_EISUP_RP_HEADER.nextval into v_id from dual; end if;
        
        p_create_time := sysdate;   
        
        insert into EISUP_RP_HEADER 
        (REPORT_ID, REP_PERIOD, REPORT_NAME, USER_NAME, 
        CREATE_TIME, START_TIME, STOP_TIME, 
        STATE, 
        DROP_REQUEST, ERR_MESSAGE, 
        PARTITION_NAME,
        REPORT_TYPE_ID, REP_PERIOD_FROM, REP_PERIOD_TO,
        FILE_NAME_1
        )
        values
        (
            v_id,p_rep_period,p_report_name,p_user_name,
            p_create_time,null,null,
            'выполняется',
            '','',
            'P_'||trim(to_char(v_id,'0000000000000000000000000')),
            p_REPORT_TYPE_ID, p_REP_PERIOD_FROM, p_REP_PERIOD_TO,
            p_FILE_NAME_1
        );
        
        gv_message:='{'||v_id||'}{'||p_REP_PERIOD||'}{'||p_REPORT_NAME||'}{'||p_USER_NAME||'}{'||p_create_time||'}{'||p_REPORT_TYPE_ID||'}{'||p_REP_PERIOD_FROM||'}{'||p_REP_PERIOD_TO||'}{'||p_FILE_NAME_1||'}';    
        pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
        
        commit;
        
        start_job(v_id);
        
        return v_id; 
   end;


    function eisup_rp_header_add_nostart(
        p_id                in number, 
        p_rep_period        in number, 
        p_report_name       in varchar2, 
        p_user_name         in varchar2,
        p_report_type_id    in number, 
        p_rep_period_from   in number ,
        p_rep_period_to     in number,
        p_file_name_1       in varchar2 default null
    ) return number
    is
        v_prcName    CONSTANT VARCHAR2(30) := 'eisup_rp_header_add';
        v_id            integer;
        p_create_time date;
    begin
        
        v_id := p_id;
        if (v_id is null) then select SQ_EISUP_RP_HEADER.nextval into v_id from dual; end if;
        
        p_create_time := sysdate;   
        
        insert into EISUP_RP_HEADER 
        (REPORT_ID, REP_PERIOD, REPORT_NAME, USER_NAME, 
        CREATE_TIME, START_TIME, STOP_TIME, 
        STATE, 
        DROP_REQUEST, ERR_MESSAGE, 
        PARTITION_NAME,
        REPORT_TYPE_ID, REP_PERIOD_FROM, REP_PERIOD_TO,
        FILE_NAME_1
        )
        values
        (
            v_id,p_rep_period,p_report_name,p_user_name,
            p_create_time,null,null,
            'выполняется',
            '','',
            'P_'||trim(to_char(v_id,'0000000000000000000000000')),
            p_REPORT_TYPE_ID, p_REP_PERIOD_FROM, p_REP_PERIOD_TO,
            p_FILE_NAME_1
        );
        
        gv_message:='{'||v_id||'}{'||p_REP_PERIOD||'}{'||p_REPORT_NAME||'}{'||p_USER_NAME||'}{'||p_create_time||'}{'||p_REPORT_TYPE_ID||'}{'||p_REP_PERIOD_FROM||'}{'||p_REP_PERIOD_TO||'}{'||p_FILE_NAME_1||'}';    
        pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
        
        return v_id; 
   end;

   procedure eisup_rp_header_start(
        p_id                in number
   )
   is 
   begin
        start_job(p_id);
   end;
   


    procedure eisup_rp_header_del(
        p_id                in number 
    )
    is
        v_prcName    CONSTANT VARCHAR2(30) := 'eisup_rp_header_del';
        v_id            integer;
        v_pr EISUP_RP_HEADER%rowtype;
    begin
        
        v_id := p_id;
        
        begin
                select * into v_pr from EISUP_RP_HEADER where  REPORT_ID = v_id;
            exception when no_data_found then
                 return;
        end;  
        
        if v_pr.state = 'выполняется' then
            raise_application_error(-20001,'отчет выполняется. удалить можно только после завершения');
        end if;

        if  v_pr.REPORT_TYPE_ID = 1 then
            begin
                execute immediate 'ALTER TABLE EISUP_RP_BODY_FOUND DROP PARTITION '||v_pr.PARTITION_NAME||'  ';
             exception when others then
                    null;
            end;
            begin
                execute immediate 'ALTER TABLE EISUP_RP_BODY_NOTFOUND DROP PARTITION '||v_pr.PARTITION_NAME||'  ';
             exception when others then
                    null;
            end;

            begin
                execute immediate 'ALTER TABLE EISUP_RP_PAYMENT_1C DROP PARTITION '||v_pr.PARTITION_NAME||'  ';
             exception when others then
                    null;
            end;
        elsif v_pr.REPORT_TYPE_ID = 2 then
            begin
                execute immediate 'ALTER TABLE EISUP_RP2 DROP PARTITION '||v_pr.PARTITION_NAME||'  ';
             exception when others then
                    null;
            end;
        elsif v_pr.REPORT_TYPE_ID = 3 then
            begin
                execute immediate 'ALTER TABLE EISUP_RP3_SF1C DROP PARTITION '||v_pr.PARTITION_NAME||'  ';
             exception when others then
                    null;
            end;
            begin
                execute immediate 'ALTER TABLE EISUP_RP3 DROP PARTITION '||v_pr.PARTITION_NAME||'  ';
             exception when others then
                    null;
            end;
        elsif v_pr.REPORT_TYPE_ID = 4 then
            begin
                execute immediate 'ALTER TABLE EISUP_RP4 DROP PARTITION '||v_pr.PARTITION_NAME||'  ';
             exception when others then
                    null;
            end;
            begin
                execute immediate 'ALTER TABLE EISUP_RP_PAYMENT_1C DROP PARTITION '||v_pr.PARTITION_NAME||'  ';
             exception when others then
                    null;
            end;
            begin
                execute immediate 'ALTER TABLE EISUP_RP4_BALANCE_1С DROP PARTITION '||v_pr.PARTITION_NAME||'  ';
             exception when others then
                    null;
            end;
        elsif v_pr.REPORT_TYPE_ID = 5 then
            begin
                execute immediate 'ALTER TABLE EISUP_RP5_SF1C DROP PARTITION '||v_pr.PARTITION_NAME||'  ';
             exception when others then
                    null;
            end;
            begin
                execute immediate 'ALTER TABLE EISUP_RP5 DROP PARTITION '||v_pr.PARTITION_NAME||'  ';
             exception when others then
                    null;
            end;
        elsif v_pr.REPORT_TYPE_ID = 6 then
            begin
                execute immediate 'ALTER TABLE EISUP_RP6 DROP PARTITION '||v_pr.PARTITION_NAME||'  ';
             exception when others then
                    null;
            end;
            begin
                execute immediate 'ALTER TABLE EISUP_RP_PAYMENT_1C DROP PARTITION '||v_pr.PARTITION_NAME||'  ';
             exception when others then
                    null;
            end;
            begin
                execute immediate 'ALTER TABLE EISUP_RP6_BALANCE_1С DROP PARTITION '||v_pr.PARTITION_NAME||'  ';
             exception when others then
                    null;
            end;
        end if;


        delete EISUP_RP_HEADER 
        where
                    REPORT_ID = v_id
        ; 
        
        gv_message:='{'||v_id||'}{'||v_pr.REP_PERIOD||'}{'||v_pr.REPORT_NAME||'}{'||v_pr.USER_NAME||'}{'||v_pr.create_time||'}';    
        pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
        
   end;

   
--    procedure eisup_rp_header_start(
--        p_id                in number 
--    )
--    is
--        v_pr EISUP_RP_HEADER%rowtype;
--        --
--        v_err  varchar2(4000);
--        p_yyyymm  number;
--    begin
--        begin
--                select * into v_pr from EISUP_RP_HEADER where  REPORT_ID = p_id;
--            exception when no_data_found then
--                 raise_application_error(-20001,'Не найдено задание id='||p_id);
--                 return;
--        end;
--        
--       if v_pr.REPORT_TYPE_ID = 1 then
--            report_type_1_start(p_id);
--       else
--            update  EISUP_RP_HEADER 
--                set 
--                    STOP_TIME   = sysdate,
--                    ERR_MESSAGE = v_err,
--                    STATE = ''
--                where REPORT_ID = p_id
--            ; 
--       end if; 
--        
--        
--        exception when others then
--            v_err   := SUBSTRB(SQLERRM, 1, 4000);
--            update  EISUP_RP_HEADER 
--                set 
--                    STOP_TIME   = sysdate,
--                    ERR_MESSAGE = 'не реализовано',
--                    STATE = 'не реализовано'
--                where REPORT_ID = p_id
--            ; 
--    end;
          
    

    procedure report_type_1_start(
        p_id                in number 
    )
    is
        v_pr EISUP_RP_HEADER%rowtype;
        --
        v_err  varchar2(4000);
        p_yyyymm  number;
    begin
        begin
                select * into v_pr from EISUP_RP_HEADER where  REPORT_ID = p_id;
            exception when no_data_found then
                 raise_application_error(-20001,'Не найдено задание id='||p_id);
                 return;
        end;
        
        begin

            update EISUP_RP_HEADER
            set 
                START_TIME = sysdate,
                STATE = 'выполняется'
            where REPORT_ID = p_id;
            commit;


            p_yyyymm :=  v_pr.REP_PERIOD;
            --
            PK10_BALANCE_EISUP_REP.LOAD_IN_BALANCE(p_yyyymm);
            PK10_BALANCE_EISUP_REP.Bind_in_balance(p_yyyymm);
            PK10_BALANCE_EISUP_REP.Bind_in_balance_BRM(p_yyyymm);
            PK10_BALANCE_EISUP_REP.Make_not_found_report(p_yyyymm);
            PK10_BALANCE_EISUP_REP.Make_not_found_report_balance(p_yyyymm);
    
            execute immediate 'ALTER TABLE EISUP_RP_BODY_FOUND ADD PARTITION '||v_pr.PARTITION_NAME||'  VALUES ('||p_id||')';
            execute immediate 'ALTER TABLE EISUP_RP_BODY_NOTFOUND ADD PARTITION '||v_pr.PARTITION_NAME||'  VALUES ('||p_id||')';
            execute immediate 'ALTER TABLE EISUP_RP_PAYMENT_1C ADD PARTITION '||v_pr.PARTITION_NAME||'  VALUES ('||p_id||')';
            
            insert into EISUP_RP_BODY_FOUND
            (REPORT_ID, AGR, LS, ERP_CODE, BALANCE_OUT, DT, DATE_V, AGR_CODE, FACTURE_NUM_LINK, AGR_CODE_1C, BALANCE_IN, BALANCE_MONTH, PAY_TOTAL, BILL_TOTAL, BRM_CONTRACT_ID, BRM_ACCOUNT_ID, BRM_COMPANY_ID, BRM_LOAD_STATUS, BRM_LOAD_NOTES, BRM_LOAD_DATE, BRM_BILLING_ID, BRM_PERIOD_ID, BRM_ACCOUNTS_NUM, BRM_ERP_CODE, LINK_BY, LINK_BY_NOTE, ERP_CONTRACT_ID, BRM_BALANS_IN, BRM_PAY_TOTAL, BRM_BILL_TOTAL, BRM_BALANS_OUT)
            select 
                p_id,
                AGR, LS, ERP_CODE, BALANCE_OUT, DT, DATE_V, AGR_CODE, FACTURE_NUM_LINK, AGR_CODE_1C, BALANCE_IN, BALANCE_MONTH, PAY_TOTAL, BILL_TOTAL, BRM_CONTRACT_ID, BRM_ACCOUNT_ID, BRM_COMPANY_ID, BRM_LOAD_STATUS, BRM_LOAD_NOTES, BRM_LOAD_DATE, BRM_BILLING_ID, BRM_PERIOD_ID, BRM_ACCOUNTS_NUM, BRM_ERP_CODE, LINK_BY, LINK_BY_NOTE, ERP_CONTRACT_ID, BRM_BALANS_IN, BRM_PAY_TOTAL, BRM_BILL_TOTAL, BRM_BALANS_OUT
            from EISUP_RP_BALANCE_TMP_1; 
            

            insert into EISUP_RP_BODY_NOTFOUND
            (REPORT_ID, CONTRACT_ID, CONTRACT_NO, CONTARCT_DATE, ERP_CONTRACT_ID, COMPANY_NAME, ERP_CODE, INN, KPP, ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, BILLING_ID, ACCOUNT_STATUS, CONTRACTOR_ID, CONTRACTOR, BRANCH_ID, BRANCH, COMPANY_FROM, COMPANY_TO, PROFILE_FROM, PROFILE_TO, MAX_PERIOD_ID, MAX_BILL_NO, BRM_BALANS_IN, BRM_PAY_TOTAL, BRM_BILL_TOTAL, BRM_BALANS_OUT)
            select 
                p_id,
                CONTRACT_ID, CONTRACT_NO, CONTARCT_DATE, ERP_CONTRACT_ID, COMPANY_NAME, ERP_CODE, INN, KPP, ACCOUNT_ID, ACCOUNT_NO, ACCOUNT_TYPE, BILLING_ID, ACCOUNT_STATUS, CONTRACTOR_ID, CONTRACTOR, BRANCH_ID, BRANCH, COMPANY_FROM, COMPANY_TO, PROFILE_FROM, PROFILE_TO, MAX_PERIOD_ID, MAX_BILL_NO, BRM_BALANS_IN, BRM_PAY_TOTAL, BRM_BILL_TOTAL, BRM_BALANS_OUT
            from EISUP_RP_NOT_FOUND_TMP; 

            insert into EISUP_RP_PAYMENT_1C
            (REPORT_ID, RECORD_ID, DOCUMENT_ID, DOC_TYPE, DOC_NO, DOC_DATE, BANK_ACCOUNT, CLNT_ACCOUNT, ERP_CODE, CONTRACT_NO, ACCOUNT_NO, DOCUMENT_NO, DOCUMENT_DATE, PAYMENT_DATE, PAYMENT_AMOUNT, CURRENCY_ID, PAY_DESCR, PERIOD, ID_JOURNAL, BANK, AGRCODE, AGRCODE1C)
            select 
                p_id,
                RECORD_ID, DOCUMENT_ID, DOC_TYPE, DOC_NO, DOC_DATE, BANK_ACCOUNT, CLNT_ACCOUNT, ERP_CODE, CONTRACT_NO, ACCOUNT_NO, DOCUMENT_NO, DOCUMENT_DATE, PAYMENT_DATE, PAYMENT_AMOUNT, CURRENCY_ID, PAY_DESCR, PERIOD, ID_JOURNAL, BANK, AGRCODE, AGRCODE1C
            from EISUP_RP_PAYMENT_1C_TMP;
            
            
            update EISUP_RP_HEADER
            set 
                STOP_TIME = sysdate,
                STATE = 'готов'
            where REPORT_ID = p_id;

            commit;
        
        exception when others then
            v_err   := SUBSTRB(SQLERRM, 1, 4000);
            update  EISUP_RP_HEADER 
                set 
                    STOP_TIME   = sysdate,
                    ERR_MESSAGE = v_err,
                    STATE = 'ошибка'
                where REPORT_ID = p_id
            ; 
        end;
          
    end;

  

    -- список договоров из 1С   
    PROCEDURE EISUP_RP_BODY_FOUND_LIST( 
        p_recordset  OUT t_refc,
        p_REPORT_ID in number 
    )
    is 
    begin
        open p_recordset for
            select 
                e.AGR, 
                e.LS, 
                e.ERP_CODE, 
                e.BALANCE_OUT, 
                e.DT, 
                e.DATE_V, 
                e.AGR_CODE, 
                e.FACTURE_NUM_LINK, 
                e.AGR_CODE_1C, 
                e.BALANCE_IN, 
                e.BALANCE_MONTH, 
                e.PAY_TOTAL, 
                e.BILL_TOTAL, 
                e.BRM_CONTRACT_ID, 
                e.BRM_ACCOUNT_ID, 
                e.BRM_COMPANY_ID, 
                e.BRM_LOAD_STATUS, 
                e.BRM_LOAD_NOTES, 
                e.BRM_LOAD_DATE, 
                e.BRM_BILLING_ID, 
                e.BRM_PERIOD_ID, 
                e.BRM_ACCOUNTS_NUM, 
                e.BRM_ERP_CODE, 
                e.LINK_BY, 
                e.LINK_BY_NOTE, 
                e.ERP_CONTRACT_ID, 
                e.BRM_BALANS_IN, 
                e.BRM_PAY_TOTAL, 
                e.BRM_BILL_TOTAL, 
                e.BRM_BALANS_OUT,
                C.CONTRACT_NO       BRM_CONTRACT_NO,
                ACC.ACCOUNT_NO      BRM_ACCOUNT_NO,
                CM.COMPANY_NAME     BRM_COMPANY_NAME,
                CM.INN              BRM_INN,
                CM.KPP              BRM_KPP
            from EISUP_RP_BODY_FOUND e,
                 CONTRACT_T c,
                 company_t cm,
                 account_t acc
            where  e.REPORT_ID = p_REPORT_ID
                   and E.BRM_CONTRACT_ID = C.CONTRACT_ID(+)
                   and E.BRM_COMPANY_ID = CM.COMPANY_ID(+)
                   and E.BRM_ACCOUNT_ID = ACC.ACCOUNT_ID(+)
            order by AGR
        ; 
    end;

    -- список EISUP_RP_BODY_NOTFOUND договоров из BRM   
    PROCEDURE EISUP_RP_BODY_NOTFOUND_LIST( 
        p_recordset  OUT t_refc,
        p_REPORT_ID in number 
    )
    is 
    begin
        open p_recordset for
            select 
                CONTRACT_ID, 
                CONTRACT_NO, 
                CONTARCT_DATE, 
                ERP_CONTRACT_ID, 
                COMPANY_NAME, 
                ERP_CODE, 
                INN, 
                KPP, 
                ACCOUNT_ID, 
                ACCOUNT_NO, 
                ACCOUNT_TYPE, 
                BILLING_ID, 
                ACCOUNT_STATUS, 
                CONTRACTOR_ID, 
                CONTRACTOR, 
                BRANCH_ID, 
                BRANCH, 
                COMPANY_FROM, 
                COMPANY_TO, 
                PROFILE_FROM, 
                PROFILE_TO, 
                MAX_PERIOD_ID, 
                MAX_BILL_NO, 
                BRM_BALANS_IN, 
                BRM_PAY_TOTAL, 
                BRM_BILL_TOTAL, 
                BRM_BALANS_OUT
            from EISUP_RP_BODY_NOTFOUND
            where  REPORT_ID = p_REPORT_ID
            order by CONTRACT_NO,ACCOUNT_NO
        ; 
    end;

    -- детализации платежей в BRM
    PROCEDURE PAY_BRM_BY_CONTRACT_LIST( 
        p_recordset  OUT t_refc,
        p_CONTRACT_ID in number,
        p_period_ID in number
    )
    is
    begin

        open p_recordset for
                SELECT * FROM (
                    SELECT P.REP_PERIOD_ID, -- id периода платежа 
                           AP.CONTRACT_ID,  -- id договора
                           A.ACCOUNT_ID,    -- id л/с
                           A.ACCOUNT_NO,    -- номер л/с
                           P.DOC_ID,        -- идентификатор документа / номер платежного поручения
                           P.PAYMENT_DATE,  -- дата платежа
                           P.RECVD,         -- сумма платежа 
                           P.PAY_DESCR,     -- описание платежа
                           P.PAYMENT_TYPE,  -- тип платежа
                           PS.PAYSYSTEM_NAME,  -- платежная сисьема, через корторую пришел платеж
                           A.BILLING_ID,    -- id биллинга в котором обслуживается л/с
                           A.ACCOUNT_TYPE,  -- тип л/с
                           A.STATUS ACCOUNT_STATUS, -- статус л/с
                           ROW_NUMBER() OVER (PARTITION BY AP.ACCOUNT_ID ORDER BY NVL(AP.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))) RN
                      FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A, PAYMENT_T P, PAYSYSTEM_T PS
                     WHERE AP.DATE_FROM < PK04_PERIOD.PERIOD_TO(p_period_id)
                       AND (AP.DATE_TO IS NULL OR PK04_PERIOD.PERIOD_FROM(p_period_id) <= AP.DATE_TO )
                       AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
                       AND AP.CONTRACT_ID = p_contract_id
                       AND P.ACCOUNT_ID   = A.ACCOUNT_ID
                       AND P.REP_PERIOD_ID = p_period_id
                       AND P.PAYSYSTEM_ID = PS.PAYSYSTEM_ID(+)
                   )
                 WHERE RN = 1
                 ORDER BY CONTRACT_ID, ACCOUNT_NO, PAYMENT_DATE
        ;
                    
    end;

    PROCEDURE PAY_BRM_BY_ACCOUNT_LIST( 
        p_recordset  OUT t_refc,
        p_ACCOUNT_ID in number,
        p_period_ID in number
    )
    is
    begin

        open p_recordset for
                SELECT * FROM (
                    SELECT P.REP_PERIOD_ID, -- id периода платежа 
                           AP.CONTRACT_ID,  -- id договора
                           A.ACCOUNT_ID,    -- id л/с
                           A.ACCOUNT_NO,    -- номер л/с
                           P.DOC_ID,        -- идентификатор документа / номер платежного поручения
                           P.PAYMENT_DATE,  -- дата платежа
                           P.RECVD,         -- сумма платежа 
                           P.PAY_DESCR,     -- описание платежа
                           P.PAYMENT_TYPE,  -- тип платежа
                           PS.PAYSYSTEM_NAME,  -- платежная сисьема, через корторую пришел платеж
                           A.BILLING_ID,    -- id биллинга в котором обслуживается л/с
                           A.ACCOUNT_TYPE,  -- тип л/с
                           A.STATUS ACCOUNT_STATUS, -- статус л/с
                           ROW_NUMBER() OVER (PARTITION BY AP.ACCOUNT_ID ORDER BY NVL(AP.DATE_TO, TO_DATE('01.01.2050','dd.mm.yyyy'))) RN
                      FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A, PAYMENT_T P, PAYSYSTEM_T PS
                     WHERE AP.DATE_FROM < PK04_PERIOD.PERIOD_TO(p_period_id)
                       AND (AP.DATE_TO IS NULL OR PK04_PERIOD.PERIOD_FROM(p_period_id) <= AP.DATE_TO )
                       AND AP.ACCOUNT_ID  = A.ACCOUNT_ID
                       --AND AP.CONTRACT_ID = p_contract_id
                       AND AP.ACCOUNT_ID = p_ACCOUNT_id
                       AND P.ACCOUNT_ID   = A.ACCOUNT_ID
                       AND P.REP_PERIOD_ID = p_period_id
                       AND P.PAYSYSTEM_ID = PS.PAYSYSTEM_ID(+)
                   )
                 WHERE RN = 1
                 ORDER BY CONTRACT_ID, ACCOUNT_NO, PAYMENT_DATE
        ;
    end;



    -- детализации счетов в BRM
    PROCEDURE BILL_BRM_BY_CONTRACT_LIST( 
        p_recordset  OUT t_refc,
        p_CONTRACT_ID in number,
        p_period_ID in number
    )
    is
    begin

        if p_period_ID < 201702 then

            open p_recordset for
                    WITH TPI AS (   -- описание выгрузки счетов в ЕИСУП
                        SELECT L.FACTURENUM,
                               H.HEADER_ID,
                               GR.NAME GROUP_NAME, 
                               GR.TABLE_NAME_H, 
                               GR.TABLE_NAME_L, 
                               H.JOURNAL_ID, 
                               H.SESSION_ID,
                               H.DATE_EXPORT_1C 
                          FROM EXPORT_1C_HEADER_T H,
                               EXPORT_1C_GROUP_T GR,
                               (
                                   SELECT HEADER_ID, FACTUREEXTERNALID FACTURENUM FROM EXPORT_1C_LINES_T
                                    UNION ALL
                                   SELECT HEADER_ID, FACTURENUM FROM EXPORT_1C_LINES_2003_T
                               ) L
                         WHERE H.HEADER_ID = L.HEADER_ID
                           AND GR.GROUP_ID = H.GROUP_ID       
                           AND H.STATUS = 'EXPORT_DATA_OK'   
                           AND (H.EXPORT_TYPE <> 'ERROR' OR H.EXPORT_TYPE IS NULL)           
                           AND H.GROUP_ID NOT IN (0,-1,99)
                           AND H.HEADER_ID IN (       
                                SELECT  HEADER_ID FROM (        
                                    SELECT HEADER_ID, ROW_NUMBER() OVER (PARTITION BY GROUP_ID,PERIOD_ID ORDER BY VERSION DESC) RN 
                                      FROM EXPORT_1C_HEADER_T H
                                     WHERE (EXPORT_TYPE <> 'ADD' OR EXPORT_TYPE IS NULL) 
                                       AND STATUS = 'EXPORT_DATA_OK'
                                )
                                WHERE RN = 1
                                UNION ALL
                                (
                                SELECT HEADER_ID
                                  FROM EXPORT_1C_HEADER_T H
                                 WHERE EXPORT_TYPE = 'ADD' 
                                   AND STATUS = 'EXPORT_DATA_OK'
                                )        
                            )
                        )
                    SELECT 
                           B.REP_PERIOD_ID,         -- id периода
                           B.CONTRACT_ID,           -- id договора
                           A.ACCOUNT_ID,            -- id л/с
                           A.ACCOUNT_NO,            -- номер л/с
                           B.BILL_NO,               -- номер счета
                           B.BILL_DATE,             -- дата счета
                           B.BILL_TYPE,             -- тип счета
                           B.BILL_STATUS,           -- статус счета
                           B.TOTAL,                 -- сумма счета
                           A.BILLING_ID,            -- id биллинга в котором обслуживается л/с
                           A.ACCOUNT_TYPE,          -- тип л/с
                           A.STATUS ACCOUNT_STATUS, -- статус л/с
                           CT.CONTRACTOR_ID,        -- id продавца
                           CT.CONTRACTOR,           -- продавец 
                           BR.CONTRACTOR_ID BRANCH_ID, -- id региона
                           BR.CONTRACTOR BRANCH,    -- регион,
                           -- координаты выгрузки в TPI ЕИСУП
                           TPI.HEADER_ID,           -- id описателя выгрузки
                           TPI.GROUP_NAME,          -- группа выгрузки
                           TPI.TABLE_NAME_H,        -- таблица куда был записан заголовок
                           TPI.TABLE_NAME_L,        -- таблица куда были записаны строки выгрузки
                           TPI.JOURNAL_ID,          -- id журнала выгрузки
                           TPI.SESSION_ID,          -- id сеанса выгрузки
                           TPI.DATE_EXPORT_1C       -- дата выгрузки в 1С
                      FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACTOR_T CT, CONTRACTOR_T BR, TPI 
                     WHERE B.REP_PERIOD_ID = p_period_id   -- 201610
                       AND B.CONTRACT_ID   = p_contract_id -- 184213448 
                       AND B.ACCOUNT_ID    = A.ACCOUNT_ID
                       AND B.CONTRACTOR_ID = CT.CONTRACTOR_ID
                       AND B.PROFILE_ID    = AP.PROFILE_ID
                       AND AP.BRANCH_ID    = BR.CONTRACTOR_ID
                       AND B.BILL_NO       = TPI.FACTURENUM(+)
                     ORDER BY B.CONTRACT_ID, A.ACCOUNT_NO, B.BILL_NO
            ;
        else
            open p_recordset for
                    WITH TPI AS (   -- описание выгрузки счетов в ЕИСУП
                        SELECT L.FACTURENUM,
                               H.HEADER_ID,
                               GR.NOTES GROUP_NAME, 
                               GR.TABLE_NAME_H, 
                               GR.TABLE_NAME_L, 
                               H.JOURNAL_ID, 
                               H.SESSION_ID,
                               H.DATE_EXPORT_1C 
                          FROM EXPORT_N_1C_HEADER_T H,
                               EXPORT_N_1C_GROUP_T GR,
                               (
                                   SELECT HEADER_ID, l.BILL_NO FACTURENUM FROM EXPORT_N_1C_LINES_T l
                                    UNION ALL
                                   SELECT HEADER_ID, FACTURENUM FROM EXPORT_1C_LINES_2003_T
                               ) L
                         WHERE H.HEADER_ID = L.HEADER_ID
                           AND GR.GROUP_ID = H.GROUP_ID       
                           AND H.STATUS = 'EXPORT_DATA_OK'   
                           AND (H.EXPORT_TYPE <> 'ERROR' OR H.EXPORT_TYPE IS NULL)           
                           AND H.GROUP_ID NOT IN (0,-1,99)
                           AND H.HEADER_ID IN (       
                                SELECT  HEADER_ID FROM (        
                                    SELECT HEADER_ID, ROW_NUMBER() OVER (PARTITION BY GROUP_ID,PERIOD_ID ORDER BY VERSION DESC) RN 
                                      FROM EXPORT_N_1C_HEADER_T H
                                     WHERE (EXPORT_TYPE <> 'ADD' OR EXPORT_TYPE IS NULL) 
                                       AND STATUS = 'EXPORT_DATA_OK'
                                )
                                WHERE RN = 1
                                UNION ALL
                                (
                                SELECT HEADER_ID
                                  FROM EXPORT_N_1C_HEADER_T H
                                 WHERE EXPORT_TYPE = 'ADD' 
                                   AND STATUS = 'EXPORT_DATA_OK'
                                )        
                            )
                        )
                    SELECT 
                           B.REP_PERIOD_ID,         -- id периода
                           B.CONTRACT_ID,           -- id договора
                           A.ACCOUNT_ID,            -- id л/с
                           A.ACCOUNT_NO,            -- номер л/с
                           B.BILL_NO,               -- номер счета
                           B.BILL_DATE,             -- дата счета
                           B.BILL_TYPE,             -- тип счета
                           B.BILL_STATUS,           -- статус счета
                           B.TOTAL,                 -- сумма счета
                           A.BILLING_ID,            -- id биллинга в котором обслуживается л/с
                           A.ACCOUNT_TYPE,          -- тип л/с
                           A.STATUS ACCOUNT_STATUS, -- статус л/с
                           CT.CONTRACTOR_ID,        -- id продавца
                           CT.CONTRACTOR,           -- продавец 
                           BR.CONTRACTOR_ID BRANCH_ID, -- id региона
                           BR.CONTRACTOR BRANCH,    -- регион,
                           -- координаты выгрузки в TPI ЕИСУП
                           TPI.HEADER_ID,           -- id описателя выгрузки
                           TPI.GROUP_NAME,          -- группа выгрузки
                           TPI.TABLE_NAME_H,        -- таблица куда был записан заголовок
                           TPI.TABLE_NAME_L,        -- таблица куда были записаны строки выгрузки
                           TPI.JOURNAL_ID,          -- id журнала выгрузки
                           TPI.SESSION_ID,          -- id сеанса выгрузки
                           TPI.DATE_EXPORT_1C       -- дата выгрузки в 1С
                      FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACTOR_T CT, CONTRACTOR_T BR, TPI 
                     WHERE B.REP_PERIOD_ID = p_period_id   -- 201610
                       AND B.CONTRACT_ID   = p_contract_id -- 184213448 
                       AND B.ACCOUNT_ID    = A.ACCOUNT_ID
                       AND B.CONTRACTOR_ID = CT.CONTRACTOR_ID
                       AND B.PROFILE_ID    = AP.PROFILE_ID
                       AND AP.BRANCH_ID    = BR.CONTRACTOR_ID
                       AND B.BILL_NO       = TPI.FACTURENUM(+)
                     ORDER BY B.CONTRACT_ID, A.ACCOUNT_NO, B.BILL_NO
            ;
        
        end if;
    end;
    
    PROCEDURE BILL_BRM_BY_ACCOUNT_LIST( 
        p_recordset  OUT t_refc,
        p_ACCOUNT_ID in number,
        p_period_ID in number
    )
    is
    begin

        if p_period_ID < 201702 then

            open p_recordset for
                    WITH TPI AS (   -- описание выгрузки счетов в ЕИСУП
                        SELECT L.FACTURENUM,
                               H.HEADER_ID,
                               GR.NAME GROUP_NAME, 
                               GR.TABLE_NAME_H, 
                               GR.TABLE_NAME_L, 
                               H.JOURNAL_ID, 
                               H.SESSION_ID,
                               H.DATE_EXPORT_1C 
                          FROM EXPORT_1C_HEADER_T H,
                               EXPORT_1C_GROUP_T GR,
                               (
                                   SELECT HEADER_ID, FACTUREEXTERNALID FACTURENUM FROM EXPORT_1C_LINES_T
                                    UNION ALL
                                   SELECT HEADER_ID, FACTURENUM FROM EXPORT_1C_LINES_2003_T
                               ) L
                         WHERE H.HEADER_ID = L.HEADER_ID
                           AND GR.GROUP_ID = H.GROUP_ID       
                           AND H.STATUS = 'EXPORT_DATA_OK'   
                           AND (H.EXPORT_TYPE <> 'ERROR' OR H.EXPORT_TYPE IS NULL)           
                           AND H.GROUP_ID NOT IN (0,-1,99)
                           AND H.HEADER_ID IN (       
                                SELECT  HEADER_ID FROM (        
                                    SELECT HEADER_ID, ROW_NUMBER() OVER (PARTITION BY GROUP_ID,PERIOD_ID ORDER BY VERSION DESC) RN 
                                      FROM EXPORT_1C_HEADER_T H
                                     WHERE (EXPORT_TYPE <> 'ADD' OR EXPORT_TYPE IS NULL) 
                                       AND STATUS = 'EXPORT_DATA_OK'
                                )
                                WHERE RN = 1
                                UNION ALL
                                (
                                SELECT HEADER_ID
                                  FROM EXPORT_1C_HEADER_T H
                                 WHERE EXPORT_TYPE = 'ADD' 
                                   AND STATUS = 'EXPORT_DATA_OK'
                                )        
                            )
                        )
                    SELECT 
                           B.REP_PERIOD_ID,         -- id периода
                           B.CONTRACT_ID,           -- id договора
                           A.ACCOUNT_ID,            -- id л/с
                           A.ACCOUNT_NO,            -- номер л/с
                           B.BILL_NO,               -- номер счета
                           B.BILL_DATE,             -- дата счета
                           B.BILL_TYPE,             -- тип счета
                           B.BILL_STATUS,           -- статус счета
                           B.TOTAL,                 -- сумма счета
                           A.BILLING_ID,            -- id биллинга в котором обслуживается л/с
                           A.ACCOUNT_TYPE,          -- тип л/с
                           A.STATUS ACCOUNT_STATUS, -- статус л/с
                           CT.CONTRACTOR_ID,        -- id продавца
                           CT.CONTRACTOR,           -- продавец 
                           BR.CONTRACTOR_ID BRANCH_ID, -- id региона
                           BR.CONTRACTOR BRANCH,    -- регион,
                           -- координаты выгрузки в TPI ЕИСУП
                           TPI.HEADER_ID,           -- id описателя выгрузки
                           TPI.GROUP_NAME,          -- группа выгрузки
                           TPI.TABLE_NAME_H,        -- таблица куда был записан заголовок
                           TPI.TABLE_NAME_L,        -- таблица куда были записаны строки выгрузки
                           TPI.JOURNAL_ID,          -- id журнала выгрузки
                           TPI.SESSION_ID,          -- id сеанса выгрузки
                           TPI.DATE_EXPORT_1C       -- дата выгрузки в 1С
                      FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACTOR_T CT, CONTRACTOR_T BR, TPI 
                     WHERE B.REP_PERIOD_ID = p_period_id   -- 201610
                       AND B.ACCOUNT_ID   = p_ACCOUNT_id -- 184213448 
                       AND B.ACCOUNT_ID    = A.ACCOUNT_ID
                       AND B.CONTRACTOR_ID = CT.CONTRACTOR_ID
                       AND B.PROFILE_ID    = AP.PROFILE_ID
                       AND AP.BRANCH_ID    = BR.CONTRACTOR_ID
                       AND B.BILL_NO       = TPI.FACTURENUM(+)
                     ORDER BY B.CONTRACT_ID, A.ACCOUNT_NO, B.BILL_NO
            ;
        else
            open p_recordset for
                    WITH TPI AS (   -- описание выгрузки счетов в ЕИСУП
                        SELECT L.FACTURENUM,
                               H.HEADER_ID,
                               GR.NOTES GROUP_NAME, 
                               GR.TABLE_NAME_H, 
                               GR.TABLE_NAME_L, 
                               H.JOURNAL_ID, 
                               H.SESSION_ID,
                               H.DATE_EXPORT_1C 
                          FROM EXPORT_N_1C_HEADER_T H,
                               EXPORT_N_1C_GROUP_T GR,
                               (
                                   SELECT HEADER_ID, BILL_NO FACTURENUM FROM EXPORT_N_1C_LINES_T
                                    UNION ALL
                                   SELECT HEADER_ID, FACTURENUM FROM EXPORT_1C_LINES_2003_T
                               ) L
                         WHERE H.HEADER_ID = L.HEADER_ID
                           AND GR.GROUP_ID = H.GROUP_ID       
                           AND H.STATUS = 'EXPORT_DATA_OK'   
                           AND (H.EXPORT_TYPE <> 'ERROR' OR H.EXPORT_TYPE IS NULL)           
                           AND H.GROUP_ID NOT IN (0,-1,99)
                           AND H.HEADER_ID IN (       
                                SELECT  HEADER_ID FROM (        
                                    SELECT HEADER_ID, ROW_NUMBER() OVER (PARTITION BY GROUP_ID,PERIOD_ID ORDER BY VERSION DESC) RN 
                                      FROM EXPORT_N_1C_HEADER_T H
                                     WHERE (EXPORT_TYPE <> 'ADD' OR EXPORT_TYPE IS NULL) 
                                       AND STATUS = 'EXPORT_DATA_OK'
                                )
                                WHERE RN = 1
                                UNION ALL
                                (
                                SELECT HEADER_ID
                                  FROM EXPORT_N_1C_HEADER_T H
                                 WHERE EXPORT_TYPE = 'ADD' 
                                   AND STATUS = 'EXPORT_DATA_OK'
                                )        
                            )
                        )
                    SELECT 
                           B.REP_PERIOD_ID,         -- id периода
                           B.CONTRACT_ID,           -- id договора
                           A.ACCOUNT_ID,            -- id л/с
                           A.ACCOUNT_NO,            -- номер л/с
                           B.BILL_NO,               -- номер счета
                           B.BILL_DATE,             -- дата счета
                           B.BILL_TYPE,             -- тип счета
                           B.BILL_STATUS,           -- статус счета
                           B.TOTAL,                 -- сумма счета
                           A.BILLING_ID,            -- id биллинга в котором обслуживается л/с
                           A.ACCOUNT_TYPE,          -- тип л/с
                           A.STATUS ACCOUNT_STATUS, -- статус л/с
                           CT.CONTRACTOR_ID,        -- id продавца
                           CT.CONTRACTOR,           -- продавец 
                           BR.CONTRACTOR_ID BRANCH_ID, -- id региона
                           BR.CONTRACTOR BRANCH,    -- регион,
                           -- координаты выгрузки в TPI ЕИСУП
                           TPI.HEADER_ID,           -- id описателя выгрузки
                           TPI.GROUP_NAME,          -- группа выгрузки
                           TPI.TABLE_NAME_H,        -- таблица куда был записан заголовок
                           TPI.TABLE_NAME_L,        -- таблица куда были записаны строки выгрузки
                           TPI.JOURNAL_ID,          -- id журнала выгрузки
                           TPI.SESSION_ID,          -- id сеанса выгрузки
                           TPI.DATE_EXPORT_1C       -- дата выгрузки в 1С
                      FROM BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, CONTRACTOR_T CT, CONTRACTOR_T BR, TPI 
                     WHERE B.REP_PERIOD_ID = p_period_id   -- 201610
                       AND B.ACCOUNT_ID   = p_ACCOUNT_id -- 184213448 
                       AND B.ACCOUNT_ID    = A.ACCOUNT_ID
                       AND B.CONTRACTOR_ID = CT.CONTRACTOR_ID
                       AND B.PROFILE_ID    = AP.PROFILE_ID
                       AND AP.BRANCH_ID    = BR.CONTRACTOR_ID
                       AND B.BILL_NO       = TPI.FACTURENUM(+)
                     ORDER BY B.CONTRACT_ID, A.ACCOUNT_NO, B.BILL_NO
            ;
        end if;
    end;

    -- детализации платежей в 1C
    PROCEDURE PAY_1C_BY_CONTRACT_LIST( 
        p_recordset  OUT t_refc,
        p_REPORT_ID   in number,
        p_CONTRACT_NO in varchar2
    )
    is
    begin

        open p_recordset for
                select
                    REPORT_ID, RECORD_ID, DOCUMENT_ID, DOC_TYPE, DOC_NO, DOC_DATE, BANK_ACCOUNT, 
                    CLNT_ACCOUNT, ERP_CODE, CONTRACT_NO, ACCOUNT_NO, DOCUMENT_NO, DOCUMENT_DATE, 
                    PAYMENT_DATE, PAYMENT_AMOUNT, CURRENCY_ID, PAY_DESCR, PERIOD, 
                    ID_JOURNAL, BANK, AGRCODE, AGRCODE1C
                from  EISUP_RP_PAYMENT_1C
                where 
                    REPORT_ID = p_REPORT_ID
                    and 
                    (p_CONTRACT_NO is null or CONTRACT_NO = p_CONTRACT_NO)
                order by PAYMENT_DATE 
                ; 
                    
    end;


    -- типы отчетов
    procedure REPORT_TYPE_list (
        p_recordset  OUT t_refc,
        p_report_group in number default null 
    )
    is 
    begin
        open p_recordset for
            select REPORT_TYPE_ID, REPORT_TYPE
            from EISUP_RP_REPORT_TYPE
            where (p_report_group is null or p_report_group = REPORT_GROUP_ID ) 
            order by REPORT_TYPE
        ; 
    end;
   
    --=====================================================================
    -- таблица связи договоров BRM с договорами 1С  (EISUP_RP_CONTR_LINK)
    --=====================================================================
    procedure BRM_1C_CONTR_LINK_list(
        p_recordset  OUT t_refc
    )
    is
    begin
        open p_recordset for
            select EISUP_RP_CONTR_LINK_ID, BRM_CONTRACT_NO, BRM_INN, TPI_CONTRACT_NO, TPI_INN
            from EISUP_RP_CONTR_LINK
            order by BRM_CONTRACT_NO,BRM_INN 
        ; 
    end;

    function BRM_1C_CONTR_LINK_add(
        p_id                in number,
        p_BRM_CONTRACT_NO     in varchar2, 
        p_BRM_INN             in varchar2,
        p_TPI_CONTRACT_NO     in varchar2, 
        p_TPI_INN             in varchar2
    ) return number
    is
        v_prcName    CONSTANT VARCHAR2(30) := 'BRM_1C_CONTR_LINK_add';
        v_id          integer;
    begin
        
        v_id := p_id;
        if (v_id is null) then select SQ_EISUP_RP.nextval into v_id from dual; end if;
        
        insert into EISUP_RP_CONTR_LINK
        (EISUP_RP_CONTR_LINK_ID, BRM_CONTRACT_NO, BRM_INN, TPI_CONTRACT_NO, TPI_INN
        )
        values
        (
            v_id,
            p_BRM_CONTRACT_NO, p_BRM_INN, p_TPI_CONTRACT_NO, p_TPI_INN
        );
        
        gv_message:='{'||v_id||'}{'||p_BRM_CONTRACT_NO||'}{'||p_BRM_INN||'}{'||p_TPI_CONTRACT_NO||'}{'||p_TPI_INN||'}';    
        pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

        return v_id; 
   end;

    procedure BRM_1C_CONTR_LINK_update(
        p_id                in number,
        p_BRM_CONTRACT_NO     in varchar2, 
        p_BRM_INN             in varchar2,
        p_TPI_CONTRACT_NO     in varchar2, 
        p_TPI_INN             in varchar2
    ) 
    is
        v_prcName    CONSTANT VARCHAR2(30) := 'BRM_1C_CONTR_LINK_update';
        v_id          integer;
        v_pr         EISUP_RP_CONTR_LINK%rowtype;
        
    begin

        v_id := p_id;
        begin
                select * into v_pr  from EISUP_RP_CONTR_LINK where  EISUP_RP_CONTR_LINK_ID = v_id;
            exception when no_data_found then
                 return;
        end;  
        
        
        update EISUP_RP_CONTR_LINK
        set
            BRM_CONTRACT_NO = p_BRM_CONTRACT_NO, 
            BRM_INN         = p_BRM_INN, 
            TPI_CONTRACT_NO = p_TPI_CONTRACT_NO, 
            TPI_INN         = p_TPI_INN
        where 
            EISUP_RP_CONTR_LINK_ID = v_id
        ;
        
        gv_message:='{'||v_id||'}{'||v_pr.BRM_CONTRACT_NO||'}{'||v_pr.BRM_INN||'}{'||v_pr.TPI_CONTRACT_NO||'}{'||v_pr.TPI_INN||'}=>'||
            '{'||v_id||'}{'||p_BRM_CONTRACT_NO||'}{'||p_BRM_INN||'}{'||p_TPI_CONTRACT_NO||'}{'||p_TPI_INN||'}';    
        pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

   end;

    procedure BRM_1C_CONTR_LINK_delete(
        p_id                in number
    ) 
    is
        v_prcName    CONSTANT VARCHAR2(30) := 'BRM_1C_CONTR_LINK_delete';
        v_id          integer;
        v_pr         EISUP_RP_CONTR_LINK%rowtype;
        
    begin

        v_id := p_id;
        begin
                select * into v_pr from EISUP_RP_CONTR_LINK where  EISUP_RP_CONTR_LINK_ID = v_id;
            exception when no_data_found then
                 return;
        end;  
        
        delete EISUP_RP_CONTR_LINK
        where 
            EISUP_RP_CONTR_LINK_ID = v_id
        ;
        
        gv_message:='{'||v_id||'}{'||v_pr.BRM_CONTRACT_NO||'}{'||v_pr.BRM_INN||'}{'||v_pr.TPI_CONTRACT_NO||'}{'||v_pr.TPI_INN||'}';
        pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);

   end;


    --=====================================================================
    -- список договоров,лицевых счетов BRM
    --=====================================================================
    procedure BRM_CONTRACT_ACC_list(
        p_recordset  OUT t_refc
    )
    is
    begin
        open p_recordset for
                select 
                                   CONTRACT_ID,   -- id договора  (можно не отображать в GUI)
                                   CONTRACT_NO,   -- номер договора
                                   CONTRACT_DATE,  -- дата договора
                                   ROW_NUMBER() OVER (PARTITION BY CONTRACT_ID ORDER BY ACCOUNT_ID) ACCOUNT_RN,
                                   ACCOUNT_ID,   -- id л/с (можно не отображать в GUI)
                                   ACCOUNT_NO,   -- номер л/с
                                   COMPANY_NAME, -- имя компании
                                   SHORT_NAME,   -- имя компании(альт)
                                   INN,          -- ИНН
                                   KPP,          -- КПП
                                   ERP_CODE,     -- ERP_CODE 
                                   CONTRACTOR,   -- продавец
                                   CONTRACTOR BRANCH, -- регион
                                   MIN_BILL_PERIOD, -- период первого счета
                                   MAX_BILL_PERIOD, -- период последнего счета
                                   BILL_COUNT,                  -- кол-во счетов
                                   BILLS_TOTAL,             -- сумма счетов
                                   BILLING_ID                         -- id биллинга
                                   --ACCOUNT_COUNT                      -- кол-во лицевых
                from
                (
                         WITH AP AS (
                                SELECT CONTRACT_ID, ACCOUNT_ID, ACCOUNT_NO, CONTRACTOR_ID, BRANCH_ID, KPP, BILLING_ID, ACCOUNT_COUNT, RN
                                  FROM (
                                      SELECT AP.CONTRACT_ID, AP.ACCOUNT_ID, A.ACCOUNT_NO, AP.CONTRACTOR_ID, AP.BRANCH_ID, AP.KPP, A.BILLING_ID,
                                             COUNT(*) OVER (PARTITION BY AP.CONTRACT_ID) ACCOUNT_COUNT,
                                             ROW_NUMBER() OVER (PARTITION BY AP.CONTRACT_ID ORDER BY A.ACCOUNT_ID) RN
                                        FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A
                                       WHERE AP.ACCOUNT_ID  = A.ACCOUNT_ID
                                         AND AP.ACTUAL      = 'Y'
                                         AND A.BILLING_ID NOT IN (2000,2003)
                                         AND A.ACCOUNT_TYPE = 'J'
                                  )   
                                 --WHERE RN = 1
                                   --AND ACCOUNT_COUNT = 1
                            )
                            SELECT 
                                   C.CONTRACT_ID,   -- id договора  (можно не отображать в GUI)
                                   C.CONTRACT_NO,   -- номер договора
                                   C.DATE_FROM CONTRACT_DATE,  -- дата договора
                                   RN ACCOUNT_RN,
                                   AP.ACCOUNT_ID,   -- id л/с (можно не отображать в GUI)
                                   AP.ACCOUNT_NO,   -- номер л/с
                                   CM.COMPANY_NAME, -- имя компании
                                   CM.SHORT_NAME,   -- имя компании(альт)
                                   CM.INN,          -- ИНН
                                   AP.KPP,          -- КПП
                                   CM.ERP_CODE,     -- ERP_CODE 
                                   CT.CONTRACTOR,   -- продавец
                                   CT.CONTRACTOR BRANCH, -- регион
                                   MIN(B.REP_PERIOD_ID) MIN_BILL_PERIOD, -- период первого счета
                                   MAX(B.REP_PERIOD_ID) MAX_BILL_PERIOD, -- период последнего счета
                                   COUNT(*) BILL_COUNT,                  -- кол-во счетов
                                   SUM(B.TOTAL) BILLS_TOTAL,             -- сумма счетов
                                   AP.BILLING_ID,                         -- id биллинга
                                   AP.ACCOUNT_COUNT                      -- кол-во лицевых
                              FROM AP, CONTRACT_T C, COMPANY_T CM, CONTRACTOR_T CT, CONTRACTOR_T BR, BILL_T B
                             WHERE C.CONTRACT_ID = CM.CONTRACT_ID
                               AND CM.ACTUAL = 'Y'
                               AND AP.CONTRACT_ID = C.CONTRACT_ID
                               AND CT.CONTRACTOR_ID = AP.CONTRACTOR_ID
                               AND BR.CONTRACTOR_ID = AP.BRANCH_ID
                               AND B.ACCOUNT_ID     = AP.ACCOUNT_ID
                               AND B.CONTRACT_ID    = AP.CONTRACT_ID 
                            GROUP BY AP.ACCOUNT_ID, AP.ACCOUNT_NO,
                                     C.CONTRACT_ID, C.CONTRACT_NO, C.DATE_FROM, 
                                     CM.COMPANY_NAME, CM.SHORT_NAME, CM.INN, AP.KPP, CM.ERP_CODE,
                                     CT.CONTRACTOR, CT.CONTRACTOR, AP.BILLING_ID,AP.ACCOUNT_COUNT, RN
                            --ORDER BY CONTRACT_NO, RN
                ) 
                order by 2,4
        ;
--         WITH AP AS (
--                SELECT CONTRACT_ID, ACCOUNT_ID, ACCOUNT_NO, CONTRACTOR_ID, BRANCH_ID, KPP, BILLING_ID, ACCOUNT_COUNT, RN
--                  FROM (
--                      SELECT AP.CONTRACT_ID, AP.ACCOUNT_ID, A.ACCOUNT_NO, AP.CONTRACTOR_ID, AP.BRANCH_ID, AP.KPP, A.BILLING_ID,
--                             COUNT(*) OVER (PARTITION BY AP.CONTRACT_ID) ACCOUNT_COUNT,
--                             ROW_NUMBER() OVER (PARTITION BY AP.CONTRACT_ID ORDER BY A.ACCOUNT_ID) RN
--                        FROM ACCOUNT_PROFILE_T AP, ACCOUNT_T A
--                       WHERE AP.ACCOUNT_ID  = A.ACCOUNT_ID
--                         AND AP.ACTUAL      = 'Y'
--                         AND A.BILLING_ID NOT IN (2000,2003)
--                         AND A.ACCOUNT_TYPE = 'J'
--                  )   
--                 --WHERE RN = 1
--                   --AND ACCOUNT_COUNT = 1
--            )
--            SELECT 
--                   C.CONTRACT_ID,   -- id договора  (можно не отображать в GUI)
--                   C.CONTRACT_NO,   -- номер договора
--                   C.DATE_FROM CONTRACT_DATE,  -- дата договора
--                   RN ACCOUNT_RN,
--                   AP.ACCOUNT_ID,   -- id л/с (можно не отображать в GUI)
--                   AP.ACCOUNT_NO,   -- номер л/с
--                   CM.COMPANY_NAME, -- имя компании
--                   CM.SHORT_NAME,   -- имя компании(альт)
--                   CM.INN,          -- ИНН
--                   AP.KPP,          -- КПП
--                   CM.ERP_CODE,     -- ERP_CODE 
--                   CT.CONTRACTOR,   -- продавец
--                   CT.CONTRACTOR BRANCH, -- регион
--                   MIN(B.REP_PERIOD_ID) MIN_BILL_PERIOD, -- период первого счета
--                   MAX(B.REP_PERIOD_ID) MAX_BILL_PERIOD, -- период последнего счета
--                   COUNT(*) BILL_COUNT,                  -- кол-во счетов
--                   SUM(B.TOTAL) BILLS_TOTAL,             -- сумма счетов
--                   AP.BILLING_ID--,                         -- id биллинга
--                   --AP.ACCOUNT_COUNT                      -- кол-во лицевых
--              FROM AP, CONTRACT_T C, COMPANY_T CM, CONTRACTOR_T CT, CONTRACTOR_T BR, BILL_T B
--             WHERE C.CONTRACT_ID = CM.CONTRACT_ID
--               AND CM.ACTUAL = 'Y'
--               AND AP.CONTRACT_ID = C.CONTRACT_ID
--               AND CT.CONTRACTOR_ID = AP.CONTRACTOR_ID
--               AND BR.CONTRACTOR_ID = AP.BRANCH_ID
--               AND B.ACCOUNT_ID     = AP.ACCOUNT_ID
--               AND B.CONTRACT_ID    = AP.CONTRACT_ID 
--            GROUP BY AP.ACCOUNT_ID, AP.ACCOUNT_NO,
--                     C.CONTRACT_ID, C.CONTRACT_NO, C.DATE_FROM, 
--                     CM.COMPANY_NAME, CM.SHORT_NAME, CM.INN, AP.KPP, CM.ERP_CODE,
--                     CT.CONTRACTOR, CT.CONTRACTOR, AP.BILLING_ID,AP.ACCOUNT_COUNT, RN
--            ORDER BY CONTRACT_NO, RN
--            ;
    end;            
    
    --=====================================================================
    -- список договоров,лицевых счетов 1C
    --=====================================================================
    procedure C1_CONTRACT_ACC_list(
        p_recordset  OUT t_refc
    )
    is
    begin
    
        PK10_BALANCE_EISUP_REP2.Load_1C_TMP;
    
        open p_recordset for
            select
                AGR, LS, ERP_CODE, BALANCE, DT, DATE_V, AGRCODE, FACTURENUMLINK, AGRCODE1C, BALANCE_NP, INN, KPP
            from EISUP_RP2_1C_TMP p
        ;

    end;

    --=====================================================================
    -- Отчет по счетам BRM, выгруженным в 1С
    --=====================================================================
    procedure BRM_BILL_list(
        p_recordset  OUT t_refc,
        p_period_id  in varchar2  -- пример : 201609
    )
    is
    begin
    
        if p_period_id < 201702 then
    
            open p_recordset for
                WITH TPL AS (
                    SELECT DISTINCT
                           HEADER_ID,
                           SUBSTR(EXECUTIONPERIOD,1,4)||SUBSTR(EXECUTIONPERIOD,6,2)  PERIOD_ID,
                           PARTNERID ERP_CODE, 
                           CUSTNAME  COMPANY_NAME, 
                           INN, KPP,
                           SALES_NAME,
                           CLIENT_SH COMPANY_SHORT,
                           AUTO_NO CONTRACT_NO,
                           ACCOUNT_NO,
                           FACTUREEXTERNALID BILL_NO,
                           net_amount,
                           tax_amount,
                           currencycode
                      FROM EXPORT_1C_LINES_T
                     WHERE EXECUTIONPERIOD = SUBSTR(p_period_id,1,4)||'.'||SUBSTR(p_period_id,5,2) --'2016.12'
                ), TPI AS (   -- описание выгрузки счетов в ЕИСУП
                    SELECT 
                           L.PERIOD_ID,         -- id периода
                           L.ERP_CODE,          -- erp_code
                           L.INN,               -- ИНН 
                           L.KPP,               -- КПП
                           L.COMPANY_NAME,      -- имя компании
                           L.COMPANY_SHORT,     -- имя компании(альт)
                           L.SALES_NAME,        --продавец
                           L.CONTRACT_NO,       --brm_contract_no
                           L.ACCOUNT_NO,        --номер л/с
                           L.BILL_NO,           --счет фактура
                           sum(l.net_amount+l.tax_amount)    summa,        -- сумма
                           l.currencycode,      -- код валюты
                           --
                           H.HEADER_ID,         -- id описателя выгрузки
                           GR.NAME  GRN ,       -- группа выгрузки
                           GR.TABLE_NAME_H,     -- таблица куда был записан заголовок
                           GR.TABLE_NAME_L,     -- таблица куда были записаны строки выгрузки
                           H.JOURNAL_ID,        -- id журнала выгрузки
                           H.SESSION_ID,        -- id сеанса выгрузки
                           H.DATE_EXPORT_1C     -- дата выгрузки в 1С
                    FROM EXPORT_1C_HEADER_T H,
                         EXPORT_1C_GROUP_T GR,
                         TPL L
                    WHERE H.HEADER_ID = L.HEADER_ID
                     AND GR.GROUP_ID = H.GROUP_ID       
                     AND H.STATUS = 'EXPORT_DATA_OK'   
                     AND (H.EXPORT_TYPE <> 'ERROR' OR H.EXPORT_TYPE IS NULL)           
                     AND H.GROUP_ID NOT IN (0,-1,99)
                     AND H.HEADER_ID IN (       
                          SELECT  HEADER_ID FROM (        
                              SELECT HEADER_ID, ROW_NUMBER() OVER (PARTITION BY GROUP_ID,PERIOD_ID ORDER BY VERSION DESC) RN 
                                FROM EXPORT_1C_HEADER_T H
                               WHERE (EXPORT_TYPE <> 'ADD' OR EXPORT_TYPE IS NULL) 
                                 AND STATUS = 'EXPORT_DATA_OK'
                          )
                          WHERE RN = 1
                          UNION ALL
                          (
                          SELECT HEADER_ID
                            FROM EXPORT_1C_HEADER_T H
                           WHERE EXPORT_TYPE = 'ADD' 
                             AND STATUS = 'EXPORT_DATA_OK'
                          )        
                      )
                    group by
                           L.PERIOD_ID,         -- id периода
                           L.ERP_CODE,          -- erp_code
                           L.INN,               -- ИНН 
                           L.KPP,               -- КПП
                           L.COMPANY_NAME,      -- имя компании
                           L.COMPANY_SHORT,     -- имя компании(альт)
                           L.SALES_NAME,        --продавец
                           L.CONTRACT_NO,       --brm_contract_no
                           L.ACCOUNT_NO,        --номер л/с
                           L.BILL_NO,           --счет фактура
                           l.currencycode,      -- код валюты
                           --
                           H.HEADER_ID,         -- id описателя выгрузки
                           GR.NAME ,       -- группа выгрузки
                           GR.TABLE_NAME_H,     -- таблица куда был записан заголовок
                           GR.TABLE_NAME_L,     -- таблица куда были записаны строки выгрузки
                           H.JOURNAL_ID,        -- id журнала выгрузки
                           H.SESSION_ID,        -- id сеанса выгрузки
                           H.DATE_EXPORT_1C     -- дата выгрузки в 1С
                )
                SELECT * FROM TPI
                ORDER BY JOURNAL_ID, CONTRACT_NO
                ;
        else

            open p_recordset for
                WITH TPL AS (
                    SELECT DISTINCT
                           HEADER_ID,
                           SUBSTR(REP_PERIOD,1,4)||SUBSTR(REP_PERIOD,6,2)  PERIOD_ID,
                           KKODE ERP_CODE, 
                           COMPANY  COMPANY_NAME, 
                           INN, KPP,
                           SALES_NAME,
                           CLIENT_SH COMPANY_SHORT,
                           CONTRACT_NO,
                           ACCOUNT_NO,
                           BILL_NO,
                           net_amount,
                           tax_amount,
                           currency_code currencycode
                      FROM EXPORT_N_1C_LINES_T
                     WHERE REP_PERIOD = SUBSTR(p_period_id,1,4)||'.'||SUBSTR(p_period_id,5,2) --'2016.12'
                ), TPI AS (   -- описание выгрузки счетов в ЕИСУП
                    SELECT 
                           L.PERIOD_ID,         -- id периода
                           L.ERP_CODE,          -- erp_code
                           L.INN,               -- ИНН 
                           L.KPP,               -- КПП
                           L.COMPANY_NAME,      -- имя компании
                           L.COMPANY_SHORT,     -- имя компании(альт)
                           L.SALES_NAME,        --продавец
                           L.CONTRACT_NO,       --brm_contract_no
                           L.ACCOUNT_NO,        --номер л/с
                           L.BILL_NO,           --счет фактура
                           sum(l.net_amount+l.tax_amount)    summa,        -- сумма
                           l.currencycode,      -- код валюты
                           --
                           H.HEADER_ID,         -- id описателя выгрузки
                           GR.NOTES ,       -- группа выгрузки
                           GR.TABLE_NAME_H,     -- таблица куда был записан заголовок
                           GR.TABLE_NAME_L,     -- таблица куда были записаны строки выгрузки
                           H.JOURNAL_ID,        -- id журнала выгрузки
                           H.SESSION_ID,        -- id сеанса выгрузки
                           H.DATE_EXPORT_1C     -- дата выгрузки в 1С
                    FROM EXPORT_N_1C_HEADER_T H,
                         EXPORT_N_1C_GROUP_T GR,
                         TPL L
                    WHERE H.HEADER_ID = L.HEADER_ID
                     AND GR.GROUP_ID = H.GROUP_ID       
                     AND H.STATUS = 'EXPORT_DATA_OK'   
                     AND (H.EXPORT_TYPE <> 'ERROR' OR H.EXPORT_TYPE IS NULL)           
                     AND H.GROUP_ID NOT IN (0,-1,99)
                     AND H.HEADER_ID IN (       
                          SELECT  HEADER_ID FROM (        
                              SELECT HEADER_ID, ROW_NUMBER() OVER (PARTITION BY GROUP_ID,PERIOD_ID ORDER BY VERSION DESC) RN 
                                FROM EXPORT_N_1C_HEADER_T H
                               WHERE (EXPORT_TYPE <> 'ADD' OR EXPORT_TYPE IS NULL) 
                                 AND STATUS = 'EXPORT_DATA_OK'
                          )
                          WHERE RN = 1
                          UNION ALL
                          (
                          SELECT HEADER_ID
                            FROM EXPORT_N_1C_HEADER_T H
                           WHERE EXPORT_TYPE = 'ADD' 
                             AND STATUS = 'EXPORT_DATA_OK'
                          )        
                      )
                    group by
                           L.PERIOD_ID,         -- id периода
                           L.ERP_CODE,          -- erp_code
                           L.INN,               -- ИНН 
                           L.KPP,               -- КПП
                           L.COMPANY_NAME,      -- имя компании
                           L.COMPANY_SHORT,     -- имя компании(альт)
                           L.SALES_NAME,        --продавец
                           L.CONTRACT_NO,       --brm_contract_no
                           L.ACCOUNT_NO,        --номер л/с
                           L.BILL_NO,           --счет фактура
                           l.currencycode,      -- код валюты
                           --
                           H.HEADER_ID,         -- id описателя выгрузки
                           GR.NOTES ,       -- группа выгрузки
                           GR.TABLE_NAME_H,     -- таблица куда был записан заголовок
                           GR.TABLE_NAME_L,     -- таблица куда были записаны строки выгрузки
                           H.JOURNAL_ID,        -- id журнала выгрузки
                           H.SESSION_ID,        -- id сеанса выгрузки
                           H.DATE_EXPORT_1C     -- дата выгрузки в 1С
                )
                SELECT * FROM TPI
                ORDER BY JOURNAL_ID, CONTRACT_NO
                ;

        end if;
    end;


    --=====================================================================
    -- Отчет по привязке договоров BRM с одним лицевым к договорам в 1С  (EISUP_RP2)
    --=====================================================================
    procedure EISUP_RP2_list(
        p_recordset  OUT t_refc,
        p_REPORT_ID in number 
    )
    is
    begin
        open p_recordset for
            select 
                BRM_CONTRACT_ID, 
                BRM_CONTRACT_NO, 
                BRM_CONTRACT_DATE, 
                BRM_ACCOUNT_ID, 
                BRM_ACCOUNT_NO, 
                BRM_COMPANY_NAME, 
                BRM_SHORT_NAME, 
                BRM_INN, 
                BRM_KPP, 
                BRM_ERP_CODE, 
                BRM_CONTRACTOR, 
                BRM_BRANCH, 
                BRM_MIN_BILL_PERIOD, 
                BRM_MAX_BILL_PERIOD, 
                BRM_BILL_COUNT, 
                BRM_BILLS_TOTAL, 
                BRM_BILLING_ID, 
                BRM_ACCOUNT_COUNT, 
                BRM_STATUS_CODE, 
                BRM_STATUS_NOTE, 
                LINK_BY, 
                LINK_BY_NOTE, 
                C1_AGR, 
                C1_LS, 
                C1_ERP_CODE, 
                C1_BALANCE,            
                C1_DT, 
                C1_DATE_V, 
                C1_AGRCODE, 
                C1_FACTURENUMLINK, 
                C1_AGRCODE1C, 
                C1_BALANCE_NP, 
                C1_INN, 
                C1_KPP
            from EISUP_RP2
            where REPORT_ID = p_REPORT_ID
            order by BRM_CONTRACT_NO
            ; 
    end;

    --=====================================================================
    -- Таблица связи договоров 1С-BRM  :    EISUP_RP_CONTRACT_T
    --=====================================================================
    procedure EISUP_RP_CONTRACT_T_list (
        p_recordset  OUT t_refc 
    )
    is
    begin
        open p_recordset for
            select 
                ce.REC_ID,
                ce.CONTRACT_ID, 
                ce.EISUP_CONTRACT_CODE, 
                ce.EISUP_CONTRACT_NO, 
                ce.EISUP_INN, 
                ce.EISUP_KPP, 
                ce.CREATE_DATE, 
                ce.CREATED_BY, 
                ce.MODYFY_DATE, 
                ce.MODYFIED_BY, 
                ce.NOTES,
                -- 
                c.contract_no
            from 
                 EISUP_RP_CONTRACT_T ce,
                 CONTRACT_T c
            where 
                 CE.CONTRACT_ID = C.CONTRACT_ID
            order by c.contract_no
            ;
    end;

    function EISUP_RP_CONTRACT_T_add(
          p_ID                   number,
          p_CONTRACT_ID          INTEGER,
          p_EISUP_CONTRACT_CODE  VARCHAR2,
          p_EISUP_CONTRACT_NO    VARCHAR2,
          p_EISUP_INN            VARCHAR2,
          p_EISUP_KPP            VARCHAR2,
          p_NOTES                VARCHAR2
    ) return number
    is
        v_prcName    CONSTANT VARCHAR2(30) := 'EISUP_RP_CONTRACT_T_add';
        v_id            integer;
        p_create_time date;
        --
        v_pr EISUP_RP_CONTRACT_T%rowtype;
    begin
        
        
        p_create_time := sysdate;   
        
        begin
            
            select * into v_pr from EISUP_RP_CONTRACT_T d
            where
                    d.CONTRACT_ID           = p_CONTRACT_ID
                and d.EISUP_CONTRACT_CODE   = p_EISUP_CONTRACT_CODE
                and d.EISUP_CONTRACT_NO     = p_EISUP_CONTRACT_NO
                and nvl(d.EISUP_INN,'-')    = nvl(p_EISUP_INN,'-')
                and nvl(d.EISUP_KPP,'-')    = nvl(p_EISUP_KPP,'-')
            ;
            v_id := v_pr.REC_ID;
        
        exception when no_data_found then    
        
            v_id := p_id;
            if (v_id is null) then select SQ_EISUP_RP_CONTRACT_T.nextval into v_id from dual; end if;

            insert into EISUP_RP_CONTRACT_T
            (
                REC_ID, 
                CONTRACT_ID, 
                EISUP_CONTRACT_CODE, 
                EISUP_CONTRACT_NO, 
                EISUP_INN, 
                EISUP_KPP, 
                CREATE_DATE, 
                CREATED_BY, 
                MODYFY_DATE, 
                MODYFIED_BY, 
                NOTES
            )
            values
            (
                v_id,
                p_CONTRACT_ID, 
                p_EISUP_CONTRACT_CODE, 
                p_EISUP_CONTRACT_NO, 
                nvl(p_EISUP_INN,'-'), 
                nvl(p_EISUP_KPP,'-'), 
                p_create_time, 
                gv_app_user, 
                p_create_time, 
                gv_app_user, 
                p_NOTES
            );
            
            gv_message:='{'||v_id||'}{'||p_CONTRACT_ID||'}{'||p_EISUP_CONTRACT_CODE||'}{'||p_EISUP_CONTRACT_NO||'}{'||p_EISUP_INN||'}{'||p_EISUP_KPP||'}{'||p_NOTES||'}{'||gv_app_user||'}';    
            pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
        
        end;
        return v_id; 
   end;

    procedure EISUP_RP_CONTRACT_T_del(
        p_id                in number 
    )
    is
        v_prcName    CONSTANT VARCHAR2(30) := 'EISUP_RP_CONTRACT_T_del';
        v_id            integer;
        v_pr EISUP_RP_CONTRACT_T%rowtype;
    begin
        
        v_id := p_id;
        
        begin
                select * into v_pr from EISUP_RP_CONTRACT_T where  REC_ID = v_id;
            exception when no_data_found then
                 return;
        end;  


        delete EISUP_RP_CONTRACT_T 
        where
                    REC_ID = v_id
        ;
        -- удалим связь так же и в таблице биллинга 
        delete CONTRACT_EISUP_T l
        where 
        l.EISUP_CONTRACT_CODE = v_pr.EISUP_CONTRACT_CODE
        and
        l.EISUP_INN = v_pr.EISUP_INN
        and
        l.EISUP_KPP = v_pr.EISUP_KPP
        ;
        
        gv_message:='{'||v_id||'}{'||v_pr.CONTRACT_ID||'}{'||v_pr.EISUP_CONTRACT_CODE||'}{'||v_pr.EISUP_CONTRACT_NO||'}{'||v_pr.EISUP_INN||'}{'||v_pr.EISUP_KPP||'}{'||v_pr.NOTES||'}{'||v_pr.CREATED_BY||'}';    
        pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
        
   end;

    --=====================================================================


    --=====================================================================
    -- Таблица связи лицевых 1С-BRM  :   EISUP_RP_CONTRACT_ACCOUNT_T
    --=====================================================================
    procedure EISUP_RP_CONTRACT_ACC_T_list (
        p_recordset  OUT t_refc 
    )
    is
    begin
        open p_recordset for
            select 
                ce.REC_ID,
                ce.CONTRACT_ID, 
                ce.ACCOUNT_ID,
                ce.EISUP_CONTRACT_CODE, 
                ce.EISUP_CONTRACT_NO, 
                ce.EISUP_INN, 
                ce.EISUP_KPP, 
                ce.CREATE_DATE, 
                ce.CREATED_BY, 
                ce.MODYFY_DATE, 
                ce.MODYFIED_BY, 
                ce.NOTES,
                -- 
                c.contract_no,
                ACC.ACCOUNT_NO
            from 
                 EISUP_RP_CONTRACT_ACCOUNT_T ce,
                 CONTRACT_T c,
                 account_t acc
            where 
                 CE.CONTRACT_ID = C.CONTRACT_ID
                 and ce.ACCOUNT_ID = acc.ACCOUNT_ID
            order by c.contract_no
            ;
    end;

    function EISUP_RP_CONTRACT_ACC_T_add(
          p_ID                   number,
          p_CONTRACT_ID          INTEGER,
          p_ACCOUNT_ID           integer,
          p_EISUP_CONTRACT_CODE  VARCHAR2,
          p_EISUP_CONTRACT_NO    VARCHAR2,
          p_EISUP_INN            VARCHAR2,
          p_EISUP_KPP            VARCHAR2,
          p_NOTES                VARCHAR2
    ) return number
    is
        v_prcName    CONSTANT VARCHAR2(30) := 'EISUP_RP_CONTRACT_ACC_T_add';
        v_id            integer;
        p_create_time date;
        --
        v_pr EISUP_RP_CONTRACT_ACCOUNT_T%rowtype;
    begin
        
        
        p_create_time := sysdate;   
        
        begin
            
            select * into v_pr from EISUP_RP_CONTRACT_ACCOUNT_T d
            where
                    d.CONTRACT_ID           = p_CONTRACT_ID
                and d.ACCOUNT_ID            = p_ACCOUNT_ID
                and d.EISUP_CONTRACT_CODE   = p_EISUP_CONTRACT_CODE
                and d.EISUP_CONTRACT_NO     = p_EISUP_CONTRACT_NO
                and nvl(d.EISUP_INN,'-')    = nvl(p_EISUP_INN,'-')
                and nvl(d.EISUP_KPP,'-')    = nvl(p_EISUP_KPP,'-')
            ;
            v_id := v_pr.REC_ID;
        
        exception when no_data_found then    
        
            v_id := p_id;
            if (v_id is null) then select SQ_EISUP_RP_CONTRACT_T.nextval into v_id from dual; end if;

            insert into EISUP_RP_CONTRACT_ACCOUNT_T
            (
                REC_ID, 
                CONTRACT_ID, 
                ACCOUNT_ID,
                EISUP_CONTRACT_CODE, 
                EISUP_CONTRACT_NO, 
                EISUP_INN, 
                EISUP_KPP, 
                CREATE_DATE, 
                CREATED_BY, 
                MODYFY_DATE, 
                MODYFIED_BY, 
                NOTES
            )
            values
            (
                v_id,
                p_CONTRACT_ID,
                p_ACCOUNT_ID, 
                p_EISUP_CONTRACT_CODE, 
                p_EISUP_CONTRACT_NO, 
                nvl(p_EISUP_INN,'-'), 
                nvl(p_EISUP_KPP,'-'), 
                p_create_time, 
                gv_app_user, 
                p_create_time, 
                gv_app_user, 
                p_NOTES
            );
            
            gv_message:='{'||v_id||'}{'||p_CONTRACT_ID||'}{'||p_ACCOUNT_ID||'}{'||p_EISUP_CONTRACT_CODE||'}{'||p_EISUP_CONTRACT_NO||'}{'||p_EISUP_INN||'}{'||p_EISUP_KPP||'}{'||p_NOTES||'}{'||gv_app_user||'}';    
            pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
        
        end;
        return v_id; 
   end;

    procedure EISUP_RP_CONTRACT_ACC_T_del(
        p_id                in number 
    )
    is
        v_prcName    CONSTANT VARCHAR2(30) := 'EISUP_RP_CONTRACT_ACC_T_del';
        v_id            integer;
        v_pr EISUP_RP_CONTRACT_ACCOUNT_T%rowtype;
    begin
        
        v_id := p_id;
        
        begin
                select * into v_pr from EISUP_RP_CONTRACT_ACCOUNT_T where  REC_ID = v_id;
            exception when no_data_found then
                 return;
        end;  
        

        delete EISUP_RP_CONTRACT_ACCOUNT_T
        where
                    REC_ID = v_id
        ; 
        
        -- удалим связь так же и в таблице биллинга 
        delete CONTRACT_EISUP_T l
        where 
        l.EISUP_CONTRACT_CODE = v_pr.EISUP_CONTRACT_CODE
        and
        l.EISUP_INN = v_pr.EISUP_INN
        and
        l.EISUP_KPP = v_pr.EISUP_KPP
        ;
        
        
        gv_message:='{'||v_id||'}{'||v_pr.CONTRACT_ID||'}{'||v_pr.ACCOUNT_ID||'}{'||v_pr.EISUP_CONTRACT_CODE||'}{'||v_pr.EISUP_CONTRACT_NO||'}{'||v_pr.EISUP_INN||'}{'||v_pr.EISUP_KPP||'}{'||v_pr.NOTES||'}{'||v_pr.CREATED_BY||'}';    
        pk01_syslog.Write_msg(gv_message,c_PkgName||'.'||v_prcName,pk01_syslog.L_normal,gv_app_user);
        
   end;

    --=====================================================================
    -- оборотно сальдовая ведомость
    --=====================================================================
    procedure oborot_sald_vedom_o(
         p_recordset  OUT t_refc,
         p_period_id in number 
    )
    is
    begin
   
        open p_recordset for   

            WITH AC AS (
              select * from 
              (
                  SELECT A.ACCOUNT_ID, A.ACCOUNT_NO, CM.COMPANY_NAME, NVL(IB.REP_PERIOD_ID, 200001) IN_PERIOD_ID ,
                            branch.CONTRACTOR region, 
                            seller.CONTRACTOR seller_name,
                            C.CONTRACT_NO,
                         ROW_NUMBER() OVER (PARTITION BY A.ACCOUNT_ID 
                                        ORDER BY AP.DATE_TO DESC NULLS FIRST, 
                                        CM.DATE_TO DESC NULLS FIRST) RN
                    FROM ACCOUNT_T A, 
                         ACCOUNT_PROFILE_T AP,
                         COMPANY_T CM,
                         PERIOD_T P,
                         INCOMING_BALANCE_T IB,
                         contractor_t branch,
                         contractor_t seller,
                         contract_t c
                   WHERE A.ACCOUNT_TYPE = 'J'
                     AND A.STATUS      != 'T'
                     AND A.ACCOUNT_ID   = AP.ACCOUNT_ID
                     AND A.ACCOUNT_ID   = IB.ACCOUNT_ID(+) 
                     AND P.PERIOD_ID    = p_period_id
                     AND AP.DATE_FROM   < P.PERIOD_TO
                     --AND CM.ACTUAL = 'Y'
                     --AND AP.ACTUAL = 'Y'
                     and branch.CONTRACTOR_ID = AP.BRANCH_ID 
                     and seller.CONTRACTOR_ID = AP.CONTRACTOR_ID
                     and C.CONTRACT_ID = AP.CONTRACT_ID
                     AND (
                            CASE 
                              WHEN AP.DATE_TO IS NOT NULL AND AP.DATE_TO > P.PERIOD_FROM THEN 1
                              WHEN AP.DATE_TO IS NULL THEN 1
                              ELSE 0
                            END
                         ) = 1
                     AND AP.CONTRACT_ID = CM.CONTRACT_ID
                     AND CM.DATE_FROM   < P.PERIOD_TO
                     AND (
                            CASE 
                              WHEN CM.DATE_TO IS NOT NULL AND CM.DATE_TO > P.PERIOD_FROM THEN 1
                              WHEN CM.DATE_TO IS NULL THEN 1
                              ELSE 0
                            END
                         ) = 1
                     --AND AP.BRANCH_ID   = p_branch_id
                     --AND ( AP.AGENT_ID  = p_agent_id OR p_agent_id IS NULL)
              ) where rn = 1 
              ), IDB AS (
                  SELECT B.ACCOUNT_ID, 
                         SUM(
                              CASE
                                WHEN B.REP_PERIOD_ID = p_period_id AND B.BILL_TYPE = 'I' THEN B.TOTAL
                                WHEN B.REP_PERIOD_ID < p_period_id THEN B.TOTAL
                                ELSE 0
                              END
                            ) IN_BILL_TOTAL
                    FROM BILL_T B, AC 
                   WHERE B.REP_PERIOD_ID <= p_period_id
                     AND B.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND B.ACCOUNT_ID     = AC.ACCOUNT_ID
                   GROUP BY B.ACCOUNT_ID
              ), ICR AS (
                  SELECT P.ACCOUNT_ID, 
                         SUM(
                              CASE
                                WHEN P.REP_PERIOD_ID = p_period_id AND P.PAYMENT_TYPE = 'INBAL' THEN P.RECVD
                                WHEN P.REP_PERIOD_ID < p_period_id THEN P.RECVD
                                ELSE 0
                              END 
                            ) IN_PAY_TOTAL 
                    FROM PAYMENT_T P, AC 
                   WHERE P.REP_PERIOD_ID <= p_period_id
                     AND P.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND P.ACCOUNT_ID     = AC.ACCOUNT_ID
                   GROUP BY P.ACCOUNT_ID
              ), BIL AS (
                  SELECT B.ACCOUNT_ID, 
                         SUM(
                              CASE
                                WHEN B.REP_PERIOD_ID = p_period_id AND B.BILL_TYPE = 'I' THEN 0
                                ELSE B.TOTAL
                              END
                            ) BILL_TOTAL
                    FROM BILL_T B, AC 
                   WHERE B.REP_PERIOD_ID  = p_period_id
                     AND B.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND B.ACCOUNT_ID     = AC.ACCOUNT_ID
                   GROUP BY B.ACCOUNT_ID
              ), PAY AS (
                  SELECT P.ACCOUNT_ID, 
                         SUM(
                              CASE
                                WHEN P.REP_PERIOD_ID = p_period_id AND P.PAYMENT_TYPE = 'INBAL' THEN 0
                                ELSE P.RECVD
                              END  
                            ) PAY_TOTAL 
                    FROM PAYMENT_T P, AC 
                   WHERE P.REP_PERIOD_ID  = p_period_id
                     AND P.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND P.ACCOUNT_ID     = AC.ACCOUNT_ID
                   GROUP BY P.ACCOUNT_ID
              )
              SELECT
                     AC.COMPANY_NAME,                    -- Клиент
                     ac.seller_name,
                     ac.region,
                     AC.ACCOUNT_NO,                      -- Лицевой счет
                     --AC.ACCOUNT_ID,  
                     NVL(IDB.IN_BILL_TOTAL, 0) IN_DEBET, -- Начальное сальдо (дебет)
                     NVL(ICR.IN_PAY_TOTAL, 0) IN_CREDIT, -- Начальное сальдо (кредит)
                     NVL(BIL.BILL_TOTAL, 0) BILL_TOTAL,  -- Оборот (дебет)
                     NVL(PAY.PAY_TOTAL, 0) PAY_TOTAL,    -- Оборот (кредит)
                     --NVL(IDB.IN_BILL_TOTAL, 0) + NVL(BIL.BILL_TOTAL, 0) OUT_DEBET, -- Конечное сальдо (дебет)
                     --NVL(ICR.IN_PAY_TOTAL, 0) + NVL(PAY.PAY_TOTAL, 0) OUT_CREDIT   -- Конечное сальдо (кредит)
                     ----
                    /**(Сальдо начальное по дебету - Сальдо начальное по кредиту) + (Обороты по дебету - Обороты по кредиту) = Сальдо конечное
                    Если сальдо с плюсом = Сальдо конечное по Дебету
                    Если сальдо с минусом =  Сальдо конечное по Кредиту
                    **/
                     case 
                        when (NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_TOTAL, 0)-NVL(PAY.PAY_TOTAL, 0))>0 then
                                (NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_TOTAL, 0)-NVL(PAY.PAY_TOTAL, 0))
                        else 0
                     end  OUT_DEBET, -- Конечное сальдо (дебет)
                     case 
                        when (NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_TOTAL, 0)-NVL(PAY.PAY_TOTAL, 0))<0 then
                                -(NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_TOTAL, 0)-NVL(PAY.PAY_TOTAL, 0))
                        else 0
                     end  OUT_CREDIT   -- Конечное сальдо (кредит)

                     ,ac.contract_no
                     ,LNK.EISUP_CONTRACT_CODE
                     --,count(*) over (partition by AC.ACCOUNT_NO) cnt
                FROM AC, IDB, ICR, BIL, PAY
                     ,CONTRACT_EISUP_T lnk
               WHERE AC.ACCOUNT_ID   = IDB.ACCOUNT_ID(+)
                 AND AC.ACCOUNT_ID   = ICR.ACCOUNT_ID(+)
                 AND AC.ACCOUNT_ID   = BIL.ACCOUNT_ID(+)
                 AND AC.ACCOUNT_ID   = PAY.ACCOUNT_ID(+)
                 --and AC.ACCOUNT_NO = 'EB156731'
                 and ac.ACCOUNT_ID = LNK.ACCOUNT_ID (+)
               --ORDER BY 10 desc, 3
               ;

    end;


    procedure oborot_sald_vedom(
         p_recordset  OUT t_refc,
         p_period_id in number 
    )
    is
    begin
   
        open p_recordset for   

            WITH AC AS (
              SELECT * FROM  
              (
                  SELECT A.ACCOUNT_ID, A.ACCOUNT_NO, CM.COMPANY_NAME, NVL(IB.REP_PERIOD_ID, 200001) IN_PERIOD_ID ,
                            BR.CONTRACTOR region, 
                            SL.CONTRACTOR seller_name,
                            C.CONTRACT_NO,
                         ROW_NUMBER() OVER (PARTITION BY A.ACCOUNT_ID 
                                        ORDER BY AP.DATE_TO DESC NULLS FIRST, 
                                        CM.DATE_TO DESC NULLS FIRST) RN
                    FROM ACCOUNT_T A, 
                         ACCOUNT_PROFILE_T AP,
                         COMPANY_T CM,
                         PERIOD_T P,
                         INCOMING_BALANCE_T IB,
                         CONTRACTOR_T BR,
                         CONTRACTOR_T SL,
                         CONTRACT_T c
                   WHERE A.ACCOUNT_TYPE = 'J'
                     AND A.STATUS      != 'T'
                     AND A.ACCOUNT_ID   = AP.ACCOUNT_ID
                     AND A.ACCOUNT_ID   = IB.ACCOUNT_ID(+) 
                     AND P.PERIOD_ID    = p_period_id
                     AND AP.DATE_FROM   < P.PERIOD_TO
                     --AND CM.ACTUAL = 'Y'
                     --AND AP.ACTUAL = 'Y'
                     and BR.CONTRACTOR_ID = AP.BRANCH_ID 
                     and SL.CONTRACTOR_ID = AP.CONTRACTOR_ID
                     and C.CONTRACT_ID = AP.CONTRACT_ID
                     AND (
                            CASE 
                              WHEN AP.DATE_TO IS NOT NULL AND AP.DATE_TO > P.PERIOD_FROM THEN 1
                              WHEN AP.DATE_TO IS NULL THEN 1
                              ELSE 0
                            END
                         ) = 1
                     AND AP.CONTRACT_ID = CM.CONTRACT_ID
                     AND CM.DATE_FROM   < P.PERIOD_TO
                     AND (
                            CASE 
                              WHEN CM.DATE_TO IS NOT NULL AND CM.DATE_TO > P.PERIOD_FROM THEN 1
                              WHEN CM.DATE_TO IS NULL THEN 1
                              ELSE 0
                            END
                         ) = 1
                     --AND AP.BRANCH_ID   = p_branch_id
                     --AND ( AP.AGENT_ID  = p_agent_id OR p_agent_id IS NULL)
              ) where rn = 1 
              ), IDB AS (
                  SELECT B.ACCOUNT_ID, 
                         SUM(
                              CASE
                                WHEN B.REP_PERIOD_ID = p_period_id AND B.BILL_TYPE = 'I' THEN B.TOTAL
                                WHEN B.REP_PERIOD_ID < p_period_id THEN B.TOTAL
                                ELSE 0
                              END
                            ) IN_BILL_TOTAL
                    FROM BILL_T B, AC 
                   WHERE B.REP_PERIOD_ID <= p_period_id
                     AND B.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND B.ACCOUNT_ID     = AC.ACCOUNT_ID
                   GROUP BY B.ACCOUNT_ID
              ), ICR AS (
                  SELECT P.ACCOUNT_ID, 
                         SUM(
                              CASE
                                WHEN P.REP_PERIOD_ID = p_period_id AND P.PAYMENT_TYPE = 'INBAL' THEN P.RECVD
                                WHEN P.REP_PERIOD_ID < p_period_id THEN P.RECVD
                                ELSE 0
                              END 
                            ) IN_PAY_TOTAL 
                    FROM PAYMENT_T P, AC 
                   WHERE P.REP_PERIOD_ID <= p_period_id
                     AND P.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND P.ACCOUNT_ID     = AC.ACCOUNT_ID
                   GROUP BY P.ACCOUNT_ID
              ), BIL AS (
                  SELECT ACCOUNT_ID, SUM(BILL_DEBET) BILL_DEBET, SUM(BILL_CREDIT) BILL_CREDIT
                    FROM (
                     SELECT B.ACCOUNT_ID, B.REP_PERIOD_ID, B.BILL_ID, 
                           B.BILL_NO, B.BILL_TYPE, BP.BILL_NO BP_BILL_NO, BP.BILL_TYPE BP_BILL_TYPE, 
                           B.TOTAL, BP.TOTAL BP_TOTAL,
                           CASE
                             WHEN B.REP_PERIOD_ID = p_period_id AND B.BILL_TYPE = 'I' 
                                THEN 0
                             WHEN BP.TOTAL IS NULL
                                THEN B.TOTAL
                             WHEN TO_CHAR(TRUNC(SYSDATE,'yyyy'),'yyyymm') <= p_period_id 
                              AND BP.TOTAL IS NOT NULL
                                THEN B.TOTAL + BP.TOTAL  
                             WHEN TO_CHAR(TRUNC(SYSDATE,'yyyy'),'yyyymm') > p_period_id 
                              AND BP.TOTAL IS NOT NULL 
                              AND (B.TOTAL + BP.TOTAL) > 0
                                THEN (B.TOTAL + BP.TOTAL)       
                             ELSE 0
                           END BILL_DEBET,
                           CASE
                             WHEN BP.TOTAL IS NULL
                                THEN 0
                             WHEN TO_CHAR(TRUNC(SYSDATE,'yyyy'),'yyyymm') > p_period_id 
                              AND BP.TOTAL IS NOT NULL 
                              AND (B.TOTAL + BP.TOTAL) < 0
                                THEN (B.TOTAL + BP.TOTAL)         
                             ELSE 0
                           END BILL_CREDIT
                      FROM BILL_T B, BILL_T BP, AC
                     WHERE B.REP_PERIOD_ID = p_period_id
                     AND B.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND B.ACCOUNT_ID     = AC.ACCOUNT_ID
                       AND B.PREV_BILL_ID  = BP.BILL_ID(+)
                       AND B.PREV_BILL_PERIOD_ID = BP.REP_PERIOD_ID(+)
                    )
                   GROUP BY ACCOUNT_ID
              ), PAY AS (
                  SELECT P.ACCOUNT_ID, 
                         SUM(
                              CASE
                                WHEN P.REP_PERIOD_ID = p_period_id AND P.PAYMENT_TYPE = 'INBAL' THEN 0
                                ELSE P.RECVD
                              END  
                            ) PAY_TOTAL 
                    FROM PAYMENT_T P, AC 
                   WHERE P.REP_PERIOD_ID  = p_period_id
                     AND P.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND P.ACCOUNT_ID     = AC.ACCOUNT_ID
                   GROUP BY P.ACCOUNT_ID
              )
              SELECT
                     AC.COMPANY_NAME,                    -- Клиент
                     AC.SELLER_NAME,
                     AC.REGION,
                     AC.ACCOUNT_NO,                      -- Лицевой счет
                     --AC.ACCOUNT_ID,  
                     NVL(IDB.IN_BILL_TOTAL, 0) IN_DEBET, -- Начальное сальдо (дебет)
                     NVL(ICR.IN_PAY_TOTAL, 0) IN_CREDIT, -- Начальное сальдо (кредит)
                     NVL(BIL.BILL_DEBET, 0)  BILL_TOTAL,   -- Оборот (дебет)
                     NVL(PAY.PAY_TOTAL, 0)+ (-NVL(BIL.BILL_CREDIT,0)) PAY_TOTAL,    -- Оборот (кредит)
                     ----
                    /**(Сальдо начальное по дебету - Сальдо начальное по кредиту) + (Обороты по дебету - Обороты по кредиту) = Сальдо конечное
                    Если сальдо с плюсом = Сальдо конечное по Дебету
                    Если сальдо с минусом =  Сальдо конечное по Кредиту
                    **/
                     CASE 
                        WHEN (NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_DEBET, 0)-(NVL(PAY.PAY_TOTAL, 0)+ (-NVL(BIL.BILL_CREDIT,0))))>0 then
                                (NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_DEBET, 0)-(NVL(PAY.PAY_TOTAL, 0)+ (-NVL(BIL.BILL_CREDIT,0))))
                        ELSE 0
                     END  OUT_DEBET, -- Конечное сальдо (дебет)
                     CASE 
                        WHEN (NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_CREDIT, 0)-(NVL(PAY.PAY_TOTAL, 0)+ (-NVL(BIL.BILL_CREDIT,0))))<0 then
                                -(NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_CREDIT, 0)-(NVL(PAY.PAY_TOTAL, 0)+ (-NVL(BIL.BILL_CREDIT,0))))
                        ELSE 0
                     END  OUT_CREDIT,  -- Конечное сальдо (кредит)
                     ----
                     AC.CONTRACT_NO,
                     LNK.EISUP_CONTRACT_CODE,
                     --
                     p_period_id   PERIOD_ID,
                     AC.ACCOUNT_ID  BRM_ACCOUNT_ID,
                     LNK.EISUP_INN,
                     LNK.EISUP_KPP,
                     LNK.EISUP_CONTRACT_NO
                     --,count(*) over (partition by AC.ACCOUNT_NO) cnt
                FROM AC, IDB, ICR, BIL, PAY, CONTRACT_EISUP_T LNK
               WHERE AC.ACCOUNT_ID   = IDB.ACCOUNT_ID(+)
                 AND AC.ACCOUNT_ID   = ICR.ACCOUNT_ID(+)
                 AND AC.ACCOUNT_ID   = BIL.ACCOUNT_ID(+)
                 AND AC.ACCOUNT_ID   = PAY.ACCOUNT_ID(+)
                 --and AC.ACCOUNT_NO = 'EB156731'
                 and AC.ACCOUNT_ID = LNK.ACCOUNT_ID(+)
               ;

    end;


    --=====================================================================
    -- оборотно сальдовая ведомость по лицевым с перенесенными балансами
    --=====================================================================
    procedure oborot_sald_vedom_moved_o1(
         p_recordset  OUT t_refc,
         p_period_id in number 
    )
    is
    begin
   
        open p_recordset for   

            WITH AC AS (
              select * from 
              (
                  SELECT A.ACCOUNT_ID, A.ACCOUNT_NO, CM.COMPANY_NAME, NVL(IB.REP_PERIOD_ID, 200001) IN_PERIOD_ID ,
                            branch.CONTRACTOR region, 
                            seller.CONTRACTOR seller_name,
                            C.CONTRACT_NO,
                         ROW_NUMBER() OVER (PARTITION BY A.ACCOUNT_ID 
                                        ORDER BY AP.DATE_TO DESC NULLS FIRST, 
                                        CM.DATE_TO DESC NULLS FIRST) RN
                    FROM ACCOUNT_T A, 
                         ACCOUNT_PROFILE_T AP,
                         COMPANY_T CM,
                         PERIOD_T P,
                         INCOMING_BALANCE_T IB,
                         contractor_t branch,
                         contractor_t seller,
                         contract_t c
                   WHERE A.ACCOUNT_TYPE = 'J'
                     AND A.STATUS      != 'T'
                     AND A.ACCOUNT_ID   = AP.ACCOUNT_ID
                     AND A.ACCOUNT_ID   = IB.ACCOUNT_ID(+) 
                     AND P.PERIOD_ID    = p_period_id
                     AND AP.DATE_FROM   < P.PERIOD_TO
                     --AND CM.ACTUAL = 'Y'
                     --AND AP.ACTUAL = 'Y'
                     and branch.CONTRACTOR_ID = AP.BRANCH_ID 
                     and seller.CONTRACTOR_ID = AP.CONTRACTOR_ID
                     and C.CONTRACT_ID = AP.CONTRACT_ID
                     AND (
                            CASE 
                              WHEN AP.DATE_TO IS NOT NULL AND AP.DATE_TO > P.PERIOD_FROM THEN 1
                              WHEN AP.DATE_TO IS NULL THEN 1
                              ELSE 0
                            END
                         ) = 1
                     AND AP.CONTRACT_ID = CM.CONTRACT_ID
                     AND CM.DATE_FROM   < P.PERIOD_TO
                     AND (
                            CASE 
                              WHEN CM.DATE_TO IS NOT NULL AND CM.DATE_TO > P.PERIOD_FROM THEN 1
                              WHEN CM.DATE_TO IS NULL THEN 1
                              ELSE 0
                            END
                         ) = 1
                     --AND AP.BRANCH_ID   = p_branch_id
                     --AND ( AP.AGENT_ID  = p_agent_id OR p_agent_id IS NULL)
              ) where rn = 1 
              ), IDB AS (
                  SELECT B.ACCOUNT_ID, 
                         SUM(
                              CASE
                                WHEN B.REP_PERIOD_ID = p_period_id AND B.BILL_TYPE = 'I' THEN B.TOTAL
                                WHEN B.REP_PERIOD_ID < p_period_id THEN B.TOTAL
                                ELSE 0
                              END
                            ) IN_BILL_TOTAL
                    FROM BILL_T B, AC 
                   WHERE B.REP_PERIOD_ID <= p_period_id
                     AND B.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND B.ACCOUNT_ID     = AC.ACCOUNT_ID
                   GROUP BY B.ACCOUNT_ID
              ), ICR AS (
                  SELECT P.ACCOUNT_ID, 
                         SUM(
                              CASE
                                WHEN P.REP_PERIOD_ID = p_period_id AND P.PAYMENT_TYPE = 'INBAL' THEN P.RECVD
                                WHEN P.REP_PERIOD_ID < p_period_id THEN P.RECVD
                                ELSE 0
                              END 
                            ) IN_PAY_TOTAL 
                    FROM PAYMENT_T P, AC 
                   WHERE P.REP_PERIOD_ID <= p_period_id
                     AND P.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND P.ACCOUNT_ID     = AC.ACCOUNT_ID
                   GROUP BY P.ACCOUNT_ID
              ), BIL AS (
                  SELECT B.ACCOUNT_ID, 
                         SUM(
                              CASE
                                WHEN B.REP_PERIOD_ID = p_period_id AND B.BILL_TYPE = 'I' THEN 0
                                ELSE B.TOTAL
                              END
                            ) BILL_TOTAL
                    FROM BILL_T B, AC 
                   WHERE B.REP_PERIOD_ID  = p_period_id
                     AND B.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND B.ACCOUNT_ID     = AC.ACCOUNT_ID
                   GROUP BY B.ACCOUNT_ID
              ), PAY AS (
                  SELECT P.ACCOUNT_ID, 
                         SUM(
                              CASE
                                WHEN P.REP_PERIOD_ID = p_period_id AND P.PAYMENT_TYPE = 'INBAL' THEN 0
                                ELSE P.RECVD
                              END  
                            ) PAY_TOTAL 
                    FROM PAYMENT_T P, AC 
                   WHERE P.REP_PERIOD_ID  = p_period_id
                     AND P.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND P.ACCOUNT_ID     = AC.ACCOUNT_ID
                   GROUP BY P.ACCOUNT_ID
              )
              SELECT
                     AC.COMPANY_NAME,                    -- Клиент
                     ac.seller_name,
                     ac.region,
                     AC.ACCOUNT_NO,                      -- Лицевой счет
                     --AC.ACCOUNT_ID,  
                     NVL(IDB.IN_BILL_TOTAL, 0) IN_DEBET, -- Начальное сальдо (дебет)
                     NVL(ICR.IN_PAY_TOTAL, 0) IN_CREDIT, -- Начальное сальдо (кредит)
                     NVL(BIL.BILL_TOTAL, 0) BILL_TOTAL,  -- Оборот (дебет)
                     NVL(PAY.PAY_TOTAL, 0) PAY_TOTAL,    -- Оборот (кредит)
                     --NVL(IDB.IN_BILL_TOTAL, 0) + NVL(BIL.BILL_TOTAL, 0) OUT_DEBET, -- Конечное сальдо (дебет)
                     --NVL(ICR.IN_PAY_TOTAL, 0) + NVL(PAY.PAY_TOTAL, 0) OUT_CREDIT   -- Конечное сальдо (кредит)
                     ----
                    /**(Сальдо начальное по дебету - Сальдо начальное по кредиту) + (Обороты по дебету - Обороты по кредиту) = Сальдо конечное
                    Если сальдо с плюсом = Сальдо конечное по Дебету
                    Если сальдо с минусом =  Сальдо конечное по Кредиту
                    **/
                     case 
                        when (NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_TOTAL, 0)-NVL(PAY.PAY_TOTAL, 0))>0 then
                                (NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_TOTAL, 0)-NVL(PAY.PAY_TOTAL, 0))
                        else 0
                     end  OUT_DEBET, -- Конечное сальдо (дебет)
                     case 
                        when (NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_TOTAL, 0)-NVL(PAY.PAY_TOTAL, 0))<0 then
                                -(NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_TOTAL, 0)-NVL(PAY.PAY_TOTAL, 0))
                        else 0
                     end  OUT_CREDIT   -- Конечное сальдо (кредит)
                     ----
                     ,ac.contract_no
                     ,LNK.EISUP_CONTRACT_CODE
                     --
                     ,p_period_id       period_id
                     ,AC.ACCOUNT_ID     brm_account_id
                     ,LNK.EISUP_INN
                     ,LNK.EISUP_KPP 
                     ,LNK.EISUP_CONTRACT_NO
                     --,count(*) over (partition by AC.ACCOUNT_NO) cnt
                FROM AC, IDB, ICR, BIL, PAY
                     ,CONTRACT_EISUP_T lnk
               WHERE AC.ACCOUNT_ID   = IDB.ACCOUNT_ID(+)
                 AND AC.ACCOUNT_ID   = ICR.ACCOUNT_ID(+)
                 AND AC.ACCOUNT_ID   = BIL.ACCOUNT_ID(+)
                 AND AC.ACCOUNT_ID   = PAY.ACCOUNT_ID(+)
                 --and AC.ACCOUNT_NO = 'EB156731'
                 and ac.ACCOUNT_ID = LNK.ACCOUNT_ID
               ;

    end;


    procedure oborot_sald_vedom_moved(
         p_recordset  OUT t_refc,
         p_period_id in number 
    )
    is
    begin
   
        OPEN p_recordset FOR   
            WITH AC AS (
                  SELECT * FROM  
                  (   -- список л/с BRM, действующих в указанном периоде 
                      SELECT A.ACCOUNT_ID, A.ACCOUNT_NO, CM.COMPANY_NAME, 
                             NVL(IB.REP_PERIOD_ID, 200001) IN_PERIOD_ID ,
                             BR.CONTRACTOR REGION, SL.CONTRACTOR SELLER_NAME, C.CONTRACT_NO,
                             P.PERIOD_ID, P.PERIOD_FROM, P.PERIOD_TO,
                             ROW_NUMBER() OVER (PARTITION BY A.ACCOUNT_ID 
                                          ORDER BY AP.DATE_TO DESC NULLS FIRST, 
                                                   CM.DATE_TO DESC NULLS FIRST) RN
                        FROM ACCOUNT_T A, 
                             ACCOUNT_PROFILE_T AP,
                             COMPANY_T CM,
                             PERIOD_T P,
                             INCOMING_BALANCE_T IB,
                             CONTRACTOR_T BR,
                             CONTRACTOR_T SL,
                             CONTRACT_T C
                       WHERE A.ACCOUNT_TYPE = 'J'
                         AND A.STATUS      != 'T'
                         AND A.ACCOUNT_ID   = AP.ACCOUNT_ID
                         AND A.ACCOUNT_ID   = IB.ACCOUNT_ID(+) 
                         AND P.PERIOD_ID    = p_period_id
                         AND AP.DATE_FROM   < P.PERIOD_TO
                         --AND CM.ACTUAL = 'Y'
                         --AND AP.ACTUAL = 'Y'
                         and BR.CONTRACTOR_ID = AP.BRANCH_ID 
                         and SL.CONTRACTOR_ID = AP.CONTRACTOR_ID
                         and C.CONTRACT_ID = AP.CONTRACT_ID
                         AND (
                                CASE 
                                  WHEN AP.DATE_TO IS NOT NULL AND AP.DATE_TO > P.PERIOD_FROM THEN 1
                                  WHEN AP.DATE_TO IS NULL THEN 1
                                  ELSE 0
                                END
                             ) = 1
                         AND AP.CONTRACT_ID = CM.CONTRACT_ID
                         AND CM.DATE_FROM   < P.PERIOD_TO
                         AND (
                                CASE 
                                  WHEN CM.DATE_TO IS NOT NULL AND CM.DATE_TO > P.PERIOD_FROM THEN 1
                                  WHEN CM.DATE_TO IS NULL THEN 1
                                  ELSE 0
                                END
                             ) = 1
                  ) WHERE RN = 1
              ), IDB AS (
                  -- сумма входящего баланса ДЕБЕТ
                  SELECT B.ACCOUNT_ID, 
                         SUM(
                              CASE
                                WHEN B.REP_PERIOD_ID = AC.PERIOD_ID AND B.BILL_TYPE = 'I' THEN B.TOTAL
                                WHEN B.REP_PERIOD_ID < AC.PERIOD_ID THEN B.TOTAL
                                ELSE 0
                              END
                            ) IN_BILL_TOTAL
                    FROM BILL_T B, AC 
                   WHERE B.REP_PERIOD_ID <= AC.PERIOD_ID
                     AND B.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND B.ACCOUNT_ID     = AC.ACCOUNT_ID
                   GROUP BY B.ACCOUNT_ID
              ), ICR AS (
                  -- сумма входящего баланса КРЕДИТ
                  SELECT P.ACCOUNT_ID, 
                         SUM(
                              CASE
                                WHEN P.REP_PERIOD_ID = AC.PERIOD_ID AND P.PAYMENT_TYPE = 'INBAL' THEN P.RECVD
                                WHEN P.REP_PERIOD_ID < AC.PERIOD_ID THEN P.RECVD
                                ELSE 0
                              END 
                            ) IN_PAY_TOTAL 
                    FROM PAYMENT_T P, AC 
                   WHERE P.REP_PERIOD_ID <= AC.PERIOD_ID
                     AND P.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND P.ACCOUNT_ID     = AC.ACCOUNT_ID
                   GROUP BY P.ACCOUNT_ID
              ), BIL AS (
                  -- обороты ДЕБЕТ указанного периода 
                  SELECT ACCOUNT_ID, SUM(BILL_DEBET) BILL_DEBET, SUM(BILL_CREDIT) BILL_CREDIT
                    FROM (
                     SELECT B.ACCOUNT_ID,
                            B.PREV_BILL_PERIOD_ID, B.PREV_BILL_ID, --BP.BILL_NO, BP.BILL_TYPE,
                            B.REP_PERIOD_ID, B.BILL_ID, B.BILL_NO, B.BILL_TYPE,
                            B.NEXT_BILL_PERIOD_ID, B.NEXT_BILL_ID, BN.BILL_NO, BN.BILL_TYPE,
                            B.TOTAL, BN.TOTAL BN_TOTAL,
                           CASE
                             -- псевдо счета входящего баланса не учитываем
                             WHEN B.REP_PERIOD_ID = AC.PERIOD_ID AND B.BILL_TYPE = 'I' 
                                THEN 0
                             -- для счетов не имеющих предистории
                             WHEN B.PREV_BILL_PERIOD_ID IS NULL
                                THEN B.TOTAL
                             -- для корректировок текущего года
                             WHEN B.PREV_BILL_PERIOD_ID/100 = B.REP_PERIOD_ID/100
                                THEN B.TOTAL + BN.TOTAL
                             -- для корректировок счетов прошедших годов, с положительной суммой после исправления
                             WHEN B.PREV_BILL_PERIOD_ID/100 < B.REP_PERIOD_ID/100
                              AND (B.TOTAL + NVL(BN.TOTAL,0)) > 0
                                THEN (B.TOTAL + NVL(BN.TOTAL,0))
                             ELSE 0
                           END BILL_DEBET,
                           CASE
                             -- для корректировок текущего года - не возникает
                             WHEN B.PREV_BILL_PERIOD_ID/100 = B.REP_PERIOD_ID/100
                                THEN 0
                             -- для корректировок счетов прошедших годов, с отрицательной суммой исправления
                             WHEN B.PREV_BILL_PERIOD_ID/100 < B.REP_PERIOD_ID/100
                              AND (B.TOTAL + NVL(BN.TOTAL,0)) < 0
                                THEN -(B.TOTAL + NVL(BN.TOTAL,0))
                             ELSE 0
                           END BILL_CREDIT
                      FROM BILL_T B, BILL_T BN, AC
                     WHERE B.NEXT_BILL_ID  = BN.BILL_ID(+)
                       AND B.NEXT_BILL_PERIOD_ID = BN.REP_PERIOD_ID(+)
                       AND (B.PREV_BILL_PERIOD_ID IS NULL OR B.PREV_BILL_PERIOD_ID < B.REP_PERIOD_ID)
                       AND B.REP_PERIOD_ID = AC.PERIOD_ID
                       AND B.REP_PERIOD_ID>= AC.IN_PERIOD_ID
                       AND B.ACCOUNT_ID    = AC.ACCOUNT_ID
                    )
                  GROUP BY ACCOUNT_ID
              ), PAY AS (
                  -- обороты КРЕДИТ указанного периода
                  SELECT P.ACCOUNT_ID, 
                         SUM(
                              CASE
                                WHEN P.REP_PERIOD_ID = AC.PERIOD_ID AND P.PAYMENT_TYPE = 'INBAL' THEN 0
                                ELSE P.RECVD
                              END  
                            ) PAY_TOTAL 
                    FROM PAYMENT_T P, AC 
                   WHERE P.REP_PERIOD_ID  = AC.PERIOD_ID
                     AND P.REP_PERIOD_ID >= AC.IN_PERIOD_ID
                     AND P.ACCOUNT_ID     = AC.ACCOUNT_ID
                   GROUP BY P.ACCOUNT_ID
              )
              SELECT
                     AC.COMPANY_NAME,                    -- Клиент
                     AC.SELLER_NAME,
                     AC.REGION,
                     AC.ACCOUNT_NO,                      -- Лицевой счет
                     --AC.ACCOUNT_ID,  
                     NVL(IDB.IN_BILL_TOTAL, 0) IN_DEBET, -- Начальное сальдо (дебет)
                     NVL(ICR.IN_PAY_TOTAL, 0) IN_CREDIT, -- Начальное сальдо (кредит)
                     NVL(BIL.BILL_DEBET, 0)  BILL_TOTAL,   -- Оборот (дебет)
                     NVL(PAY.PAY_TOTAL, 0)+ NVL(BIL.BILL_CREDIT,0) PAY_TOTAL,    -- Оборот (кредит)
                     ----
                    /**(Сальдо начальное по дебету - Сальдо начальное по кредиту) + (Обороты по дебету - Обороты по кредиту) = Сальдо конечное
                    Если сальдо с плюсом = Сальдо конечное по Дебету
                    Если сальдо с минусом =  Сальдо конечное по Кредиту
                    **/
                     CASE 
                        WHEN (NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_DEBET, 0)-(NVL(PAY.PAY_TOTAL, 0)+ (-NVL(BIL.BILL_CREDIT,0))))>0 then
                                (NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_DEBET, 0)-(NVL(PAY.PAY_TOTAL, 0)+ (-NVL(BIL.BILL_CREDIT,0))))
                        ELSE 0
                     END  OUT_DEBET, -- Конечное сальдо (дебет)
                     CASE 
                        WHEN (NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_CREDIT, 0)-(NVL(PAY.PAY_TOTAL, 0)+ (-NVL(BIL.BILL_CREDIT,0))))<0 then
                                -(NVL(IDB.IN_BILL_TOTAL, 0)-NVL(ICR.IN_PAY_TOTAL, 0)+NVL(BIL.BILL_CREDIT, 0)-(NVL(PAY.PAY_TOTAL, 0)+ (-NVL(BIL.BILL_CREDIT,0))))
                        ELSE 0
                     END  OUT_CREDIT,  -- Конечное сальдо (кредит)
                     ----
                     AC.CONTRACT_NO,
                     LNK.EISUP_CONTRACT_CODE,
                     --
                     AC.PERIOD_ID   PERIOD_ID,
                     AC.ACCOUNT_ID  BRM_ACCOUNT_ID,
                     LNK.EISUP_INN,
                     LNK.EISUP_KPP,
                     LNK.EISUP_CONTRACT_NO
                FROM AC, IDB, ICR, BIL, PAY, CONTRACT_EISUP_T LNK
               WHERE AC.ACCOUNT_ID   = IDB.ACCOUNT_ID(+)
                 AND AC.ACCOUNT_ID   = ICR.ACCOUNT_ID(+)
                 AND AC.ACCOUNT_ID   = BIL.ACCOUNT_ID(+)
                 AND AC.ACCOUNT_ID   = PAY.ACCOUNT_ID(+)
                 AND AC.ACCOUNT_ID   = LNK.ACCOUNT_ID--(+)
               ;

    end;


    --=====================================================================
    -- детализации платежей в 1C для :
    -- оборотно сальдовая ведомость по лицевым с перенесенными балансами
    --=====================================================================
    PROCEDURE PAY_1C_for_OSV_moved( 
        p_recordset  OUT t_refc,
        --
        p_YYYYMM in number,
        p_contract_code in varchar2,
        p_eisup_inn in varchar2,
        p_eisup_kpp in varchar2
    )
    is
    begin

        delete EISUP_RP_PAYMENT_1C_TMP; 

        FOR l_cur IN (
            SELECT  
                p."record_id" record_id, p.DOCUMENT_ID, p.DOC_TYPE, p.DOC_NO, 
                 p.DOC_DATE, p.bank_account, p.clnt_account, p.erp_code
                 ,p.contract_no, p.account_no, p.document_no, p.document_date, p.payment_date, p.payment_amount,
                 p.currency_id, p.pay_descr, p."period" period, p."id_journal" id_journal, p."bank" bank, 
                 p."AgrCode" AgrCode, p."AgrCode1C" AgrCode1C,
                 p."EISUP_INN" EISUP_INN,
                 p."EISUP_KPP" EISUP_KPP
            FROM dbo.payment_for_brm@TPI.WORLD p
            WHERE 
                p."period" = p_YYYYMM
                and 
                (p_contract_code is null or p."AgrCode1C" = p_contract_code)
                and
                (p_eisup_inn is null or p."EISUP_INN" = p_eisup_inn)
                and
                (p_eisup_kpp is null or p."EISUP_KPP" = p_eisup_kpp)
        ) LOOP                          
            INSERT INTO PIN.EISUP_RP_PAYMENT_1C_TMP (
                   record_id, document_id, doc_type, doc_no, doc_date, bank_account, 
                   clnt_account, erp_code, contract_no, account_no, document_no, 
                   document_date, payment_date, 
                   payment_amount, currency_id, pay_descr, period, id_journal, bank, agrcode, agrcode1c,
                   EISUP_INN, EISUP_KPP
                   ) 
            VALUES (l_cur.record_id, l_cur.document_id, l_cur.doc_type, l_cur.doc_no, TO_DATE(l_cur.doc_date,'YYYY-MM-DD'), l_cur.bank_account,
                    l_cur.clnt_account, l_cur.erp_code, l_cur.contract_no, l_cur.account_no, l_cur.document_no, 
                    TO_DATE(l_cur.document_date,'YYYY-MM-DD'), TO_DATE(l_cur.payment_date,'YYYY-MM-DD'), 
                    l_cur.payment_amount,l_cur.currency_id, l_cur.pay_descr, l_cur.period, l_cur.id_journal, l_cur.bank, l_cur.agrcode, 
                    trim(l_cur.agrcode1c),
                    nvl(trim(l_cur.EISUP_INN),'-'),
                    nvl(trim(l_cur.EISUP_KPP),'-')
                    );        
        END LOOP;


        open p_recordset for
                select
                    RECORD_ID, DOCUMENT_ID, DOC_TYPE, DOC_NO, DOC_DATE, BANK_ACCOUNT, 
                    CLNT_ACCOUNT, ERP_CODE, CONTRACT_NO, ACCOUNT_NO, DOCUMENT_NO, DOCUMENT_DATE, 
                    PAYMENT_DATE, PAYMENT_AMOUNT, CURRENCY_ID, PAY_DESCR, PERIOD, 
                    ID_JOURNAL, BANK, AGRCODE, AGRCODE1C,EISUP_INN, EISUP_KPP
                from  EISUP_RP_PAYMENT_1C_TMP
                order by PAYMENT_DATE
        ; 
    end;



    --=====================================================================
    -- детализации платежей в BRM запериод (отчет по просьбе Макеева от 06.03.2017)
    --=====================================================================
    procedure report_pay_brm (
        p_recordset  OUT t_refc,
        --
        --p_period_id in number
        p_date_from in date,
        p_date_to in date
   )
    is
    begin

        if to_char(p_date_from,'YYYYMM') != to_char(p_date_to,'YYYYMM') then
            raise_application_error(-20001,'Диапазон дат должен принадлежать одному отчетному периоду');
        end if;

        open p_recordset for

            WITH PP AS (
            SELECT P.REP_PERIOD_ID, 
                   P.PAYMENT_DATE, 
                   P.PAYMENT_ID,   
                   P.RECVD, 
                   P.TRANSFERED, 
                   P.CURRENCY_ID, 
                   CR.CURRENCY_CODE, 
                   P.PAYMENT_TYPE, 
                   P.DOC_ID, 
                   P.PAY_DESCR, 
                   P.NOTES,
                   P.CREATED_BY,
                   P.PAYSYSTEM_ID, 
                   PS.PAYSYSTEM_NAME,
                   P.ACCOUNT_ID, 
                   A.ACCOUNT_NO, 
                   A.ACCOUNT_TYPE, 
                   A.BILLING_ID, 
                   AP.PROFILE_ID,
                   C.CONTRACT_NO,
                   CE.EISUP_CONTRACT_CODE,
                   CM.ERP_CODE, 
                   CM.INN, 
                   AP.KPP, 
                   CM.COMPANY_NAME
                   
              FROM PAYMENT_T P, PAYSYSTEM_T PS, ACCOUNT_T A, ACCOUNT_PROFILE_T AP, 
                   CONTRACT_T C, COMPANY_T CM, CURRENCY_T CR,
                   CONTRACT_EISUP_T CE
             WHERE P.REP_PERIOD_ID = to_char(p_date_from,'YYYYMM') --p_period_id
               AND P.PAYSYSTEM_ID  = PS.PAYSYSTEM_ID
               AND P.ACCOUNT_ID    = A.ACCOUNT_ID(+)
               --AND A.BILLING_ID   != 2003
               AND P.PAYMENT_DATE >= p_date_from 
               AND P.PAYMENT_DATE <  trunc(p_date_to)+1
               AND P.ACCOUNT_ID    = AP.ACCOUNT_ID(+)
               AND AP.CONTRACT_ID  = C.CONTRACT_ID(+)  
               AND AP.ACTUAL(+)    ='Y' 
               AND AP.CONTRACT_ID  = CM.CONTRACT_ID(+)
               AND CM.ACTUAL(+)    ='Y'
               AND P.CURRENCY_ID   = CR.CURRENCY_ID
               AND P.ACCOUNT_ID    = CE.ACCOUNT_ID(+)
            )
            SELECT 
                    PAYMENT_ID       --"ID платежа"
                   ,REP_PERIOD_ID    --"Период"
                   ,PAYMENT_DATE     --"Дата платежа"
                   ,RECVD            --"Сумма"
                   ,TRANSFERED       --"Разнесено на счета"
                   ,CURRENCY_CODE    --"Валюта"
                   ,PAYMENT_TYPE     --"Тип платежа"
                   ,DOC_ID           --"Документ"
                   ,PAY_DESCR        --"Описание платежа"
                   ,NOTES            --"Примечание"
                   ,PAYSYSTEM_NAME   --"Платежная система"
                   ,CONTRACT_NO      --"№ договора"
                   ,EISUP_CONTRACT_CODE --"Код договора в 1С"
                   ,ACCOUNT_NO       --"№ л/с"
                   ,ACCOUNT_TYPE     --"Тип л/с"
                   ,ERP_CODE         --"Код контрагента"
                   ,INN              --"ИНН"
                   ,KPP              --"КПП"
                   ,COMPANY_NAME     --"Плательщик"
                   ,CREATED_BY       --"Менеджер"
              FROM PP
             WHERE PP.BILLING_ID   != 2003
               AND PP.ACCOUNT_TYPE != 'P'
             ORDER BY PAYSYSTEM_NAME, CONTRACT_NO
        ;
    end;

END ;
/
