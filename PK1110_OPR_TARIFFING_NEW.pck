CREATE OR REPLACE PACKAGE PK1110_OPR_TARIFFING_NEW
IS

    gc_PkgName       CONSTANT varchar2(36) := 'PK1110_OPR_TARIFFING_NEW';
    gc_MaxDate       CONSTANT date := TO_DATE('01.01.2050','dd.mm.yyyy'); 
    gc_MinDate       CONSTANT date := TO_DATE('01.01.1900','dd.mm.yyyy');    

    gc_Zone_Def_SPb_Id CONSTANT number := 65; -- id ���� � DEF-������ ������������� 
    gc_Zone_Def_Nvr_Id CONSTANT number := 60; -- id ���� � DEF-������ �������������

    -- �������� ������ ��� ����������� SPb
    TYPE rec_Trf_Op IS RECORD (Cdr_Id                number,
                               BDR_Type_Id           number,
                               Start_Time            date,
                               Duration              number,
                               Sw_Name               varchar2(34),
                               Tg_In                 varchar2(34),
                               Tg_Out                varchar2(34),
                               Abn_A                 varchar2(34),
                               Abn_B                 varchar2(34),
                               CDR_Abn_A             varchar2(34),
                               Called_Number         varchar2(34),
                               order_a_swtg_id       number,
                               Order_Id_A            varchar2(32),
                               Order_Ph_A            varchar2(34),
                               Local_Time_A          date,
                               UTC_Offset_A          INTERVAL DAY(0) TO SECOND(0),
                               order_b_swtg_id       number,
                               Order_Id_B            varchar2(32),
                               Order_Ph_B            varchar2(34),
                               Local_Time_B          date,
                               UTC_Offset_B          INTERVAL DAY(0) TO SECOND(0),                                                              
                               row_id                rowid
                              );
                                  
    TYPE ref_Trf_Op IS REF CURSOR RETURN rec_Trf_Op;

    /* ------------------------------------------------------------------------------   
      ��������� ��� �������� � ���������� ���������� XTTK
      ������� ���������:
         p_Data_Type - ��� ������ (SPB, NOVTK)
         p_Date_From - ���� ������ ��������������� �������
         p_Date_To   - ���� ����� ��������������� �������
         p_Load_Items - TRUE - ������������� item-�. ��������, ������������ ��-���������. Item-� ��������������� � ����������� � ������
                                    ������������   
                        FALSE - �� ������������� item-�                                    
      ����������: ��� ����������� �������� ������ ����������� �� ��� � ����������� ��� �� ����, ��� ��� ������
          ����� ������� �� ���������                         
      ------------------------------------------------------------------------------- */                           
    PROCEDURE Trf_Op_XTTK(p_Data_Type      IN varchar2,
                          p_Date_From      IN DATE     DEFAULT TRUNC(SYSDATE) - 1,  -- ���� ������ ��������������� �������
                          p_Date_To        IN DATE     DEFAULT TRUNC(SYSDATE) - 1/86400,  -- ���� ����� ��������������� �������
                      --    p_Load_Res   IN BOOLEAN  DEFAULT FALSE,
                          p_Test_BDR_Table IN varchar2 DEFAULT NULL,
                          p_Load_Items     IN BOOLEAN  DEFAULT TRUE
                         );  

    /*
     ��������� ��� �������� ����������� ������ � BDR � ���������� ��������������� ������ � CDR. (������� �)
       ������� ���������:
           p_Data_Type  - ��� ������ (SPB, NOVTK)
           p_Side       - �������������� ������� (� ��� B)
           p_Data_Table - ������� � ������������ �������, ������� ���� ����������������� � ��������� � �DR-�
           p_Day        - �������������� ����. �.�. ��� ����, ���������� �� �������
                          ��������� � ����., ��� ������� �������� � ��������� p_Data_Table
           p_Rep_Period - �������� ������, ���� ���� �������� BDR-�. ���� NULL - �� �������.
           p_Task_Id    - ������������� �������. ������������ ������ ��� �������� ��������� �����                       
    */
    PROCEDURE Load_BDR_XTTK(p_Data_Type      IN varchar2,
                            p_Data_Table     IN VARCHAR2,
                            p_Date_From      IN DATE,
                            p_Date_To        IN DATE,
                            p_Rep_Period     IN OUT DATE,
                            p_Task_Id        IN NUMBER   DEFAULT NULL,
                            p_Test_BDR_Table IN varchar2 DEFAULT NULL
                           );

    PROCEDURE Recalc_Op_V_Tariff(p_Data_Type   IN varchar2,
                                 p_Rep_Period  IN date,
                                 p_Modify_Date IN date DEFAULT NULL);
                                 
    FUNCTION Trf_Oper_Table(pr_Call     ref_Trf_Op,
                            p_Period_Id number,
                            p_Task_Id   number
                           )
                          RETURN PIN.OP_BDR_COLL
                          PIPELINED PARALLEL_ENABLE (PARTITION pr_Call BY ANY);                                 

END PK1110_OPR_TARIFFING_NEW;
/
CREATE OR REPLACE PACKAGE BODY PK1110_OPR_TARIFFING_NEW
IS

    TYPE t_Varch IS TABLE OF varchar2(255) INDEX BY varchar2(16);
    TYPE t_NumV  IS TABLE OF number        INDEX BY varchar2(100);

    t_BDR_Status   t_Varch;
    t_X07_Srv      t_NumV;
    t_Switch       t_NumV;
    t_Prefix       t_Varch; -- ������ �����������
    
       
    TYPE t_BDR_Rec IS RECORD(bdr_code varchar2(32),
                             network_code varchar2(32)
                            );
    
    TYPE t_BDR IS TABLE OF t_BDR_rec INDEX BY varchar2(16); 
        
    t_BDR_Network  t_BDR;  -- �������������� BDR - ����
    
    TYPE r_SubSrv IS RECORD(service_id number,
                            parent_subsrv_id number
                           ); 
    -- ��� ��������� ������ �� ������ ������ X07_SRV_DCT � SubService_t                       
    TYPE t_SubSrv IS TABLE OF r_SubSrv INDEX BY varchar2(16);
    
    t_X07_SubSrv t_SubSrv;
    
    gl_Max_Prefix  number;

/* ------------------------------------------------------------------------------   
  ��������� ��� �������� � ���������� ���������� XTTK
  ������� ���������:
     p_Data_Type - ��� ������ (SPB, NOVTK)
     p_Date_From - ���� ������ ��������������� �������
     p_Date_To   - ���� ����� ��������������� �������
     p_Load_Items - TRUE - ������������� item-�. ��������, ������������ ��-���������. Item-� ��������������� � ����������� � ������
                                ������������   
                    FALSE - �� ������������� item-�                           
  ����������: ��� ����������� �������� ������ ����������� �� ��� � ����������� ��� �� ����, ��� ��� ������
      ����� ������� �� ���������                         
  ------------------------------------------------------------------------------- */                           
PROCEDURE Trf_Op_XTTK(p_Data_Type      IN varchar2,
                      p_Date_From      IN DATE     DEFAULT TRUNC(SYSDATE) - 1,  -- ���� ������ ��������������� �������
                      p_Date_To        IN DATE     DEFAULT TRUNC(SYSDATE) - 1/86400,  -- ���� ����� ��������������� �������
                  --    p_Load_Res   IN BOOLEAN  DEFAULT FALSE,
                      p_Test_BDR_Table IN varchar2 DEFAULT NULL,
                      p_Load_Items     IN BOOLEAN  DEFAULT TRUE
                     )  
IS
    v_prcName         CONSTANT VARCHAR2(16) := 'Trf_Op_XTTK';
    
    l_Tmp_Table       VARCHAR2(32);
    l_Date_From       DATE := p_Date_From;
    l_Date_To         DATE;
   
    l_Sid             NUMBER;
    l_Rep_Period      date;
    l_Prev_Rep_Period date;
    l_Update          number;
    
    l_Days_Cnt   NUMBER := TRUNC(p_Date_To) - TRUNC(p_Date_From) + 1; -- ���-�� ����, �������� � �������� ������ (��� �����)
    l_Curr_Day   NUMBER := 0; -- ������� ������������������� ���� (��� �����)
    
BEGIN

   -- �������� ���� �� �������� ������� � �������� ��������� ���
    IF pk114_items.Check_Rep_Period(p_Date_From, p_Date_To, NULL) = 0
    THEN
        RETURN;
    END IF;     

  /* ������� ������� �� ���� ������� - ��� ��������� �� �������� ������ 
   -- �������� ������������� ������ �� ������� ���������, ������������ ��� �����������
   -- pk1001_resources.Load_Oper_Service_Ph;
    IF p_Load_Res = TRUE THEN
        
    END IF;*/    

    ------------------------------------------------------------------------------------------------------
    -- ���������� ���������� ��� ������ RS, ����� ������������� �� ��������� �� ����� ������ ���������
  --  mdv.pk21_lock.LOCK_RESOURCE(p_Mode      => DBMS_LOCK.SX_MODE,
  --                              p_Lock_Name => mdv.pk21_lock.c_Lock_RS);
    --
    ------------------------------------------------------------------------------------------------------

    Pk01_Syslog.Write_Msg(p_Msg => 'Begin period: ' || TO_CHAR(p_Date_From,'dd.mm.yyyy hh24:mi:ss') || 
                                   ' - ' || TO_CHAR(p_Date_To,'dd.mm.yyyy hh24:mi:ss') ||
                                   ', Src: ' || p_Data_Type,     
                          p_Src => gc_PkgName || '.' || v_prcName);    
      
    l_Curr_Day := l_Curr_Day + 1;
    
    --
    --- �������� SID ������
    SELECT SID INTO l_SID
      FROM v$mystat
     WHERE ROWNUM = 1;

    -- �������� ���� ��������� ������� ��� � �������� ������� 
    l_Date_To := LEAST(TRUNC(l_Date_From + 1) - 1/86400, p_Date_To);    
        
    LOOP  -- ������� ���� � �������� �������
        EXIT WHEN l_Date_From > p_Date_To; -- �����, ���� ��� � �������� ������� ���������

       ---
       -- ���� ����� ������ �� ���������� ������� �, �� ������� ����������/����������� ���������� ������
       --
        -- ��������� ��� �������� ������� (SYSDATE + SID), ��� ����� ��������� ������ �� ����������� CDR-�
        -- � ������ �������    
        l_Tmp_Table := 'QT' || TO_CHAR(SYSDATE,'ddmmyyyyhh24miss') || TO_CHAR(l_SID);
       
       -- ����������� ����������     
        pk120_Bind_Operators_new.Bind_XTTK_Opers(p_Data_Type  => p_Data_Type,
                                                 p_Date_From  => l_Date_From,
                                                 p_Date_To    => l_Date_To,
                                                 p_Pivot_Tbl  => NULL,
                                                 p_Result_Tbl => l_Tmp_Table,
                                                 p_Upd_CDR    => FALSE,
                                                 p_Id_Log     => 0,
                                                 p_Full_Bind  => (CASE WHEN p_Test_BDR_Table IS NOT NULL THEN TRUE
                                                                       ELSE FALSE
                                                                  END)                                                                
                                                );                
        
       -- ����������� 
        l_Rep_Period := NULL;
        
        PK1110_OPR_TARIFFING_NEW.Load_BDR_XTTK(p_Data_Type      => p_Data_Type,
                                           p_Data_Table     => l_Tmp_Table,
                                           p_Date_From      => l_Date_From,
                                           p_Date_To        => l_Date_To,
                                           p_Rep_Period     => l_Rep_Period,
                                           p_Task_Id        => 0,
                                           p_Test_BDR_Table => p_Test_BDR_Table
                                          );        
                                    
        -- ������������� �������� ������ �� ������� ������
        IF p_Test_BDR_Table IS NULL THEN  

            l_Update := pk120_Bind_Operators_new.Update_CDR_Op_Id(
                                                              p_Data_Table   => l_Tmp_Table,
                                                              p_Date_From  => l_Date_From,
                                                              p_Date_To    => l_Date_To,
                                                              p_Id_Log       => 0
                                                             );
            
            pin.Pk01_Syslog.Write_Msg(p_Msg => 'CDR updated: ' || TO_CHAR(l_Update), 
                                      p_Src => 'pk120_Bind_Operators_new.Update_CDR_Op_Id');
           
            IF TRUNC(l_Rep_Period,'mm') != l_Prev_Rep_Period AND l_Prev_Rep_Period IS NOT NULL 
            THEN
            
                Recalc_Op_V_Tariff(p_Data_Type   => p_Data_Type,
                                   p_Rep_Period  => l_Prev_Rep_Period,
                                   p_Modify_Date => SYSDATE);
                                     
                IF p_Load_Items = TRUE THEN                    
                   -- ���������� ����� ������, �.�. �������� �������� ������
                   pk114_items.Load_BDR_to_Item(p_Data_Type  => p_Data_Type,
                                                p_Period     => l_Prev_Rep_Period,
                                                p_Account_Id => NULL);     
                                                     
                   pk114_items.Load_Op_MinPay(p_Data_Type  => p_Data_Type,
                                              p_Rep_Period => l_Prev_Rep_Period,
                                              p_Call_Month => NULL
                                             );                        
                END IF;                                                                                                                 
               
                l_Prev_Rep_Period := l_Rep_Period;
                 
            ELSIF l_Prev_Rep_Period IS NULL THEN
                 
                 l_Prev_Rep_Period := TRUNC(l_Rep_Period,'mm');
            END IF;     
             
        END IF;
        
        /*
        -- ����������� ��� ������� 
        EXECUTE IMMEDIATE
            'UPDATE ' || p_BDR_Table || ' b ' ||
            '   SET b.trf_type = pk110_tariffing.Get_Samara_Trf_Type(b.abn_b, b.dir_b_id) ' ||
            ' WHERE rep_period BETWEEN :l_Date_From AND :l_Date_To ' ||
            '   AND b.dir_b_id IS NOT NULL ' ||
            '   AND b.trf_type IS NULL '  
        USING l_Date_From, TRUNC(l_Date_From)+2-1/86400; -- +2 ����� ������ ������� */    
        
        COMMIT;

       -- ������� ������������� �������
        EXECUTE IMMEDIATE 'DROP TABLE ' || l_Tmp_Table || ' PURGE ';

        -- ����� ������� ������� ���������� ������� ��������� ��� �� �������� �� ���������� �������� � ��� 
  /*      IF mdv.pk21_lock.Check_Lock_Req(p_Mode      => DBMS_LOCK.SX_MODE, 
                                        p_Lock_Name => mdv.pk21_lock.c_LOCK_RS) > 0 
        THEN
            -- ������� ��� ������� ����������
            mdv.pk21_lock.Unlock_Resource; 
            -- ���� ���� �� ����� �������� �������� � ����� ������� �����������
            mdv.pk21_lock.Wait_Req_Lock(p_Mode      => DBMS_LOCK.SX_MODE,
                                        p_Lock_Name => mdv.pk21_lock.c_LOCK_RS); 
            -- ������������� ����������
            mdv.pk21_lock.LOCK_RESOURCE(p_Mode      => DBMS_LOCK.SX_MODE,
                                        p_Lock_Name => mdv.pk21_lock.c_LOCK_RS);
        END IF;    */

        l_Date_From := l_Date_To + 1/86400;
        l_Date_To   := LEAST(TRUNC(l_Date_From) + 1 - 1/86400,  p_Date_To);        

        l_Curr_Day := l_Curr_Day + 1;

    END LOOP; -- ������� ���� � �������� �������

    -- ������� ������������� ����������        
 --   mdv.pk21_lock.UNLOCK_RESOURCE; 

    -- ������������� �������� ������ 
    IF p_Test_BDR_Table IS NULL THEN    
    
        Recalc_Op_V_Tariff(p_Data_Type   => p_Data_Type,
                           p_Rep_Period => l_Rep_Period,
                           p_Modify_Date => SYSDATE);

        IF p_Load_Items = TRUE THEN
        
           -- ���������� ����� ������, �.�. �������� �������� ������
            pk114_items.Load_BDR_to_Item(p_Data_Type  => p_Data_Type,
                                         p_Period     => l_Rep_Period,
                                         p_Account_Id => NULL);          
                                             
            pk114_items.Load_Op_MinPay(p_Data_Type  => p_Data_Type,
                                       p_Rep_Period => l_Rep_Period,
                                       p_Call_Month => NULL
                                      );                        
        END IF;                                             
                            
    END IF;                            
   
    Pk01_Syslog.Write_Msg(p_Msg => 'Period was calculated successfully. Src: ' || p_Data_Type || ', ' || 
                                   TO_CHAR(p_Date_From,'dd.mm.yyyy hh24:mi:ss') || 
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
        Pk01_Syslog.Write_to_log(p_Msg => '������. � �������� ������� �������� p_Bill �� ����� ���� FALSE.', 
                                 p_Src => c_PkgName || v_prcName);
    WHEN OTHERS THEN
        mdv.pk21_lock.UNLOCK_RESOURCE;    
        Pk01_Syslog.Err_to_log(p_src => c_PkgName || v_prcName);
        RAISE;         */
END Trf_Op_XTTK;                   
   

/*
 ��������� ��� �������� ����������� ������ � BDR � ���������� ��������������� ������ � CDR. (������� �)
   ������� ���������:
       p_Data_Type  - ��� ������ (SPB, NOVTK)
       p_Data_Table - ������� � ������������ �������, ������� ���� ����������������� � ��������� � �DR-�
       p_Day        - �������������� ����. �.�. ��� ����, ���������� �� �������
                      ��������� � ����., ��� ������� �������� � ��������� p_Data_Table
       p_Rep_Period - �������� ������, ���� ���� �������� BDR-�. ���� NULL - �� �������.
       p_Task_Id    - ������������� �������. ������������ ������ ��� �������� ��������� �����                       
*/
PROCEDURE Load_BDR_XTTK(p_Data_Type      IN varchar2,
                        p_Data_Table     IN VARCHAR2,
                        p_Date_From      IN DATE,
                        p_Date_To        IN DATE,
                        p_Rep_Period     IN OUT DATE,
                        p_Task_Id        IN NUMBER   DEFAULT NULL,
                        p_Test_BDR_Table IN varchar2 DEFAULT NULL
                       )
IS
    v_prcName       CONSTANT VARCHAR2(24) := 'Load_BDR_XTTK';
    
    TYPE t_Item_Id  IS TABLE OF PIN.ITEM_T.ITEM_ID%TYPE;
    
    lt_Item_Id      t_Item_Id;
    
    l_BDR_Table     VARCHAR2(32);
    l_Calc_Date     DATE   := SYSDATE;
    l_BDR_Type_Id   number;
    l_InsBDRCnt     PLS_INTEGER;
    l_Count         number;
    l_Lock_Name     VARCHAR2(64);
    l_Rep_Period    DATE;
    l_Rep_Period_Id number;
    l_BType_List    VARCHAR2(8);
   
    l_SQL VARCHAR2(4000);
    
    ERR_NET EXCEPTION;
    
BEGIN

   --- 
   ---- ========================================================================
   ---  �������� ��,�� � ������� ���������� � ������� BDR-��
   ---  ========================================================================   
   ---

   -- �������� ��� ������� BDR-��, ������������ ��� ������� ���� �������� �
   -- ��� bdr-��, ��� ������� ����� ������� ������������������� ������
    l_BDR_Type_Id := PIN.Get_BDR_Type(p_Data_Type => p_Data_Type,
                                      p_BDR_Table => l_BDR_Table,  -- out
                                      p_Agent     => l_Count,     -- out (����� �������� �� �����)
                                      p_Items     => l_InsBDRCnt   -- out (����� �������� �� �����)
                                     );       

    l_InsBDRCnt := 0;

    IF p_Test_BDR_Table IS NOT NULL THEN
    
        l_BDR_Table := p_Test_BDR_Table;
    
    END IF;                  
                  
    IF p_Test_BDR_Table IS NULL THEN
       --- ��������� ���������� ��������, ���� ����� ������� ��������� ����������. �������� ����� 
       -- ��� ������������� ������� �� ���������� ������ �������.
        l_Lock_Name := mdv.Lock_Partition(p_Owner     => SUBSTR(l_BDR_Table,1,INSTR(l_BDR_Table,'.')-1),
                                          p_Table     => SUBSTR(l_BDR_Table,INSTR(l_BDR_Table,'.')+1),
                                          p_Day       => p_Date_From,
                                          p_Add_Value => NULL -- ������� � ��������������
                                         );    
    END IF;                                     
    
-- �������� �������� ������  
    IF p_Rep_Period IS NULL THEN    
        
        p_Rep_Period := PK114_ITEMS.Get_Period_Date(p_Day       => p_Date_From,
                                                    p_Calc_Date => l_Calc_Date 
                                                   );        
        
    ELSE
       -- ����� �������� ������. �������� ������������ (������/�� ������).
        CASE pin.Check_Cls_Period(p_Rep_Period, l_Calc_Date) 
            WHEN 1 THEN
                Pk01_Syslog.Write_Msg(p_Msg   => '�������� �������� ������ ������.',
                                      p_src   => gc_PkgName || '.' || v_prcName,
                                      p_Level => Pk01_Syslog.L_err);
                RETURN;                      
            WHEN 2 THEN                           
                Pk01_Syslog.Write_Msg(p_Msg   => '� �������� �������� ������� ����� ��� �� ������������.',
                                      p_src   => gc_PkgName || '.' || v_prcName,
                                      p_Level => Pk01_Syslog.L_err);
                RETURN;                                                                
            ELSE
               -- ������ ���������. �������� � ������� ��� ������ ��� ��������.  
                IF TRUNC(p_Rep_Period,'mm') != TRUNC(p_Date_From,'mm') THEN      
                    p_Rep_Period := TRUNC(p_Rep_Period,'mm');
                ELSIF TRUNC(p_Rep_Period,'mm') = TRUNC(p_Date_From,'mm') THEN
                    p_Rep_Period := TRUNC(p_Date_From);
                END IF;   
                
        END CASE;            
            
    END IF;
    
    IF TRUNC(p_Rep_Period,'mm') = TRUNC(p_Date_From,'mm') THEN
       -- ��� �������� ���������� ��������. ���� �������� NULL, �� �������� � BDR-��
       -- ����� ������������ �������� ����������
        l_Rep_Period := NULL;
    ELSE
        l_Rep_Period := p_Rep_Period;    
    END IF;        
    
    l_Rep_Period_Id := TO_NUMBER(TO_CHAR(p_Rep_Period,'YYYYMM'));
    
    --- ����������� � �������� ����������� ��, �� � ������� ���������� 
    l_SQL :=   
       'INSERT INTO BDR_OPER_TMP ( ' || CHR(10) ||
        '       REP_PERIOD, SAVE_DATE, MODIFY_DATE, BDR_TYPE_ID, TRF_TYPE, BDR_STATUS, ' || CHR(10) ||
        '       CDR_ID, START_TIME, LOCAL_TIME, UTC_OFFSET, DURATION, ' || CHR(10) ||
        '       ABN_A, ABN_B, ACCOUNT_ID, ORDER_ID, AMOUNT, TARIFF, ' || CHR(10) || 
        '       BILL_ID, BILL_MINUTES, ITEM_ID, ' || CHR(10) || 
        '       ORDER_SWTG_ID, OP_RATE_PLAN_ID, RATEPLAN_ID, PRICE_ID, ' || CHR(10) || 
        '       SERVICE_ID, SUBSERVICE_ID, PARENT_SUBSRV_ID, VOL_TYPE, ORDER_BODY_ID,' || CHR(10) || 
        '       SW_NAME, TRUNK_GROUP_IN, TRUNK_GROUP_OUT, PREF_B, TERM_Z_NAME) ' || CHR(10) || 
       '     SELECT NVL(:l_Rep_Period, x.bill_date) rep_period, ' ||
           '        :l_Save_Date calc_date, :l_Save_Date modify_date, ' || CHR(10) || 
           '        x.BDR_Type_Id, x.trf_type, x.BDR_Status, ' || 
           '        x.cdr_id, x.start_time, x.bill_date, x.utc_offset, x.duration, ' || CHR(10) ||
           '        x.abn_a, x.abn_b, x.account_id, x.Order_Id, x.Amount, x.Price, ' || CHR(10) ||
           '        x.bill_id, x.bill_minutes, NULL item_id, ' || CHR(10) ||
           '        x.order_swtg_id, x.op_rate_plan_id, x.rateplan_id, x.price_id, ' || CHR(10) ||
           '        x.service_id, x.subservice_id, x.parent_subsrv_id, x.vol_type, x.order_body_id, ' || CHR(10) ||
           '        x.sw_name, x.tg_in, x.tg_out, x.pref_b, x.term_z_name ' || CHR(10) ||
       '       FROM TABLE(CAST(PK1110_OPR_TARIFFING_NEW.Trf_Oper_Table( ' || CHR(10) ||
              '    CURSOR(SELECT /*+ PARALLEL(t 5) */ ' || CHR(10) ||
              '                  t.cdr_id, t.bdr_type_id, t.start_time, ' || CHR(10) ||
              '                  t.duration, t.Sw_Name, t.trunk_group_in, t.trunk_group_out, ' || CHR(10) ||
              '                  t.subs_a, t.subs_b, t.abn_a, t.called_number, ' || CHR(10) ||
              '                  t.order_a_swtg_id, t.order_id_a, t.gln_a order_ph_a, t.local_time_a, t.utc_offset_a, ' || CHR(10) ||
              '                  t.order_b_swtg_id, t.order_id_b, t.gln_b order_ph_b, t.local_time_b, t.utc_offset_b, ' || CHR(10) ||
              '                  t.row_id ' || CHR(10) ||
              '             FROM ' || p_Data_Table || ' t ' || CHR(10) ||
            --  '            WHERE t.order_id_a >= 0 ' || CHR(13) ||  
            --  '               OR t.order_id_b >= 0 ' || CHR(13) ||
            --  '             AND cdr_id = 225735318 ' ||
            --  '            ORDER BY 2 ASC ' ||
              '         ), :p_Rep_Period_Id, :p_Task_Id) AS OP_BDR_COLL) ' || CHR(13) || 
              '       ) x ';
                      
      --  INSERT INTO MDV.MS_SQL VALUES(0,l_SQL);
      --  COMMIT;
                      
    EXECUTE IMMEDIATE l_SQL                 
      USING l_Rep_Period,  
            l_Calc_Date, l_Calc_Date, 
            l_Rep_Period_Id, p_Task_Id;    
            
    l_InsBdrCnt := SQL%ROWCOUNT; --/2;        

    IF p_Task_Id > 0 THEN
    
       -- ������� CDR-�, �� ���������� ��������� �� �������� ��������, �.�.
       -- ���� ����������� � BDR-� ����� �� �������� � �������� � ������� 
        DELETE FROM BDR_OPER_TMP bd 
         WHERE NOT EXISTS
                       (SELECT 1  
                          FROM Q01_RETRF_JOB_DETAIL q   
                         WHERE 1=1
                           AND q.task_id = p_Task_Id
                           AND q.order_id_new = bd.order_id
                         --  AND (q.bill_id IS NULL OR q.bill_id = bd.bill_id) -- ��� ������� �� �������� 
                       );    

        l_Count := SQL%ROWCOUNT;
                    
        IF l_Count > 0 THEN

            l_InsBdrCnt := l_InsBdrCnt - l_Count;

            Pk01_Syslog.Write_Msg(p_Msg => '������� �������. �������: ' || TO_CHAR(p_Date_from,'dd.mm.yyyy hh24:mi:ss') ||
                                           ' - ' || TO_CHAR(p_Date_To,'dd.mm.yyyy hh24:mi:ss') ||
                                           ', Src: ' || p_Data_Type ||    
                                           ', ���-��: ' || TO_CHAR(l_Count) ||
                                           ', Task_Id: ' || TO_CHAR(p_Task_Id), 
                                  p_Src => gc_PkgName || '.' || v_prcName);
        END IF;                             

    END IF;
     
   -- ��������/�������� - ���� ���� ��������� ����� BDR-� � �������� �����
    IF p_Test_BDR_Table IS NULL THEN
    
       -- ������� ������, ������� ������ � �������� �����
        EXECUTE IMMEDIATE
            'DELETE FROM BDR_OPER_TMP b ' || CHR(10) ||
            ' WHERE b.trf_type IN (' || pk114_items.Get_List_BDR_Types(p_Data_Type,NULL,'D') || ')' || CHR(10) || -- ������ ����� �������� � �����
            '   AND b.bill_id IN (:l_Bill_Closed, :l_Bill_NC)'        
        USING pk00_const.c_BILL_IS_CLOSED, -- -16
              pk00_const.c_BILL_NOT_CORRECT; -- -17

        l_Count := SQL%ROWCOUNT;

        IF l_Count > 0 THEN

            l_InsBdrCnt := l_InsBdrCnt - l_Count;

            Pk01_Syslog.Write_Msg(p_Msg   => '������� �� �������� � �������� �����: ' || TO_CHAR(p_Date_From,'dd.mm.yyyy hh24:mi:ss') ||
                                             ' - ' || TO_CHAR(p_Date_To,'dd.mm.yyyy hh24:mi:ss') ||            
                                             ', Src: ' || p_Data_Type ||    
                                             ', �������: ' || TO_CHAR(l_Count) ||
                                             ', Task_Id: ' || TO_CHAR(p_Task_Id), 
                                  p_Src   => gc_PkgName || '.' || v_prcName,
                                  p_Level => Pk01_Syslog.L_warn);
        END IF;             
      
    
      --- +++++++++++++++++++++++++++++++++++++++++++++++++++++
      -- ��������� � �������� ������ �/�����, ������� ����� ���������� ��� �����������
      -- (��������� ������� �� ����� � ��� �� ��������)
        l_Count := 0;
      
        FOR idx IN 1 .. 2
        LOOP
           -- ��� idx = 1 ��������� ������ ������� �, ��� 2 - B
            -- �������� ������ ����� ��� ������� �������
          l_BType_List := pk114_items.Get_List_BDR_Types(p_Data_Type => p_Data_Type,
                                                         p_Side      => (CASE idx WHEN 1 THEN 'A'
                                                                                  WHEN 2 THEN 'B'
                                                                         END),
                                                         p_In_Out    => NULL  -- D - �����, � - ������
                                                        );      
                                                        
          -- ��������� ������ � ������� ��������, ������� ���������� ��� ����������� 
          -- (��� ����������� ����������� ���������� ������� �������� � CDR-��)  
          EXECUTE IMMEDIATE
             'MERGE INTO ' || p_Data_Table || ' d ' || CHR(10) ||  
             'USING (SELECT b.order_id, t.rowid rd ' || CHR(10) ||
             '         FROM (SELECT b.cdr_id, b.order_id ' || CHR(10) ||
             '                 FROM BDR_OPER_TMP b ' || CHR(10) ||
             '                WHERE b.bdr_status  >= 0 ' || CHR(10) || -- ������� �����������������
             '                  AND b.bdr_type_id = :l_BDR_Type ' || CHR(10) ||
             '                  AND b.trf_type IN (' || l_BType_List || ')' || CHR(10) ||    
             '                GROUP BY b.cdr_id, b.order_id ' || CHR(10) ||
             '                HAVING COUNT(1) = 1 ' || CHR(10) || -- ��������� ����� ��������� ������� �� ����� ��������
                             -- � �� ���� �� ���������������� �������. ��������� � CDR-�� ������
             '              ) b, ' || CHR(10) ||       
                            p_Data_Table || ' t ' || CHR(10) ||
             '        WHERE t.order_id_a IS NOT NULL ' || CHR(10) ||         
             '          AND b.cdr_id = t.cdr_id ' || CHR(10) ||
             '          AND b.order_id != t.order_id_a ' || CHR(10) ||
             '        GROUP BY b.order_id, t.rowid ' || CHR(10) ||
             '      ) bt ' || CHR(10) ||
             ' ON (d.rowid = bt.rd) ' || CHR(10) ||
             ' WHEN MATCHED THEN UPDATE ' || CHR(10) ||
             '  SET ' || (CASE idx WHEN 1 THEN 'd.order_id_a'
                                   WHEN 2 THEN 'd.order_id_b'
                          END) || ' = bt.order_id'
          USING l_BDR_Type_Id;
      
          l_Count := l_Count + SQL%ROWCOUNT;
      
        END LOOP;              
    
        IF l_Count > 0 THEN

            Pk01_Syslog.Write_Msg(p_Msg => '�������� ID ������� ��� �����������: ' || TO_CHAR(p_Date_From,'dd.mm.yyyy') ||
                                           ', Src: ' || p_Data_Type ||    
                                           ', ���-��: ' || TO_CHAR(l_Count) ||
                                           ', Task_Id: ' || TO_CHAR(p_Task_Id), 
                                  p_Src => gc_PkgName || '.' || v_prcName);
        END IF;                
    
        l_Count := 0; 
        
       --- +++++++++++++++++++++++++++++++++++++++++++++++++++++ 
       -- ������� ������ 
       
       --- 1. ������� �
        -- ������������� ������ ������ item-��, bdr-� ������� ����� �������
        EXECUTE IMMEDIATE                         
            'UPDATE item_t i ' || CHR(10) ||
            '   SET item_status = :l_Error ' || CHR(10) ||
            ' WHERE i.rep_period_id = :l_Rep_Period_Id ' || CHR(10) ||
            '   AND i.item_status = :l_Open ' || CHR(10) ||
            '   AND i.item_id IN (SELECT /*++ parallel(b 5) */ ' ||
            '                            b.item_id ' || CHR(10) ||
            '                       FROM ' || l_BDR_Table || ' b, ' ||
                                         p_Data_Table || ' t ' || CHR(10) || 
            '                    WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
            '                      AND b.bdr_type_id  = :l_BDR_Type ' || CHR(10) ||      
            '                      AND b.trf_type IN (' || pk114_items.Get_List_BDR_Types(p_Data_Type,'A','D') || ')' || CHR(10) ||
            '                      AND t.order_id_a IS NOT NULL ' || CHR(10) ||   
            '                      AND t.cdr_id = b.cdr_id ' || CHR(10) ||       
            '                      AND (b.bill_id < 0 OR b.bill_id IS NULL ' ||
                                     '   OR ' ||
                                     '  EXISTS (SELECT 1 ' || CHR(10) ||
                                     '            FROM BILL_T bl ' || CHR(10) ||
                                     '           WHERE bl.rep_period_id = :l_Rep_Period_Id ' || CHR(10) || 
                                     '             AND bl.bill_status IN (:l_Open) ' || CHR(10) ||
                                     '             AND bl.bill_id = b.bill_id) ' || CHR(10) ||
                                     ' ) ' || CHR(10) ||
            '      ) '                                         
        USING pk00_const.c_ITEM_STATE_ERROR,
              l_Rep_Period_Id,                
              pk00_const.c_ITEM_STATE_OPEN,
              NVL(l_Rep_Period, p_Date_From), NVL(l_Rep_Period, p_Date_To),
              l_BDR_Type_Id,
              l_Rep_Period_Id,
              pk00_const.c_BILL_STATE_OPEN;       
       
       -- ������� ������ BDR-�
        EXECUTE IMMEDIATE
            'DELETE /*++ parallel(b 5) */ FROM ' || l_BDR_Table || ' b ' || CHR(10) ||
            ' WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
            '   AND b.bdr_type_id = :l_BDR_Type ' || CHR(10) ||
            '   AND b.trf_type IN (' || pk114_items.Get_List_BDR_Types(p_Data_Type,'A') || ')' ||                     
            '   AND (b.bill_id < 0 OR b.bill_id IS NULL ' ||
                    '  OR ' ||
                    ' EXISTS (SELECT 1 ' || CHR(10) ||
                    '           FROM BILL_T bl ' || CHR(10) ||
                    '          WHERE bl.rep_period_id = :l_Rep_Period_Id ' || CHR(10) || 
                    '            AND bl.bill_status IN (:l_Open) ' || CHR(10) ||
                    '            AND bl.bill_id = b.bill_id) ' || CHR(10) ||
            '      ) ' || CHR(10) ||        
            '   AND  EXISTS (SELECT 1 ' || CHR(10) ||
            '                  FROM '|| p_Data_Table || ' t '|| CHR(10) ||
            '                 WHERE t.order_id_a IS NOT NULL ' || CHR(10) ||   
            '                   AND t.cdr_id = b.cdr_id) '
        USING NVL(l_Rep_Period, p_Date_From), NVL(l_Rep_Period, p_Date_To),
              l_BDR_Type_Id, 
              l_Rep_Period_Id,
              pk00_const.c_BILL_STATE_OPEN;

        l_Count := SQL%ROWCOUNT;      
        
        
       --- 2. ������� �
       
        -- ������������� ������ ������ item-��, bdr-� ������� ����� �������
        EXECUTE IMMEDIATE                         
            'UPDATE item_t i ' || CHR(10) ||
            '   SET item_status = :l_Error ' || CHR(10) ||
            ' WHERE i.rep_period_id = :l_Rep_Period_Id ' || CHR(10) ||
            '   AND i.item_status = :l_Open ' || CHR(10) ||
            '   AND i.item_id IN (SELECT /*++ parallel(b 5) */ ' ||
            '                            b.item_id ' || CHR(10) ||
            '                       FROM ' || l_BDR_Table || ' b, ' ||
                                         p_Data_Table || ' t ' || CHR(10) || 
            '                    WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
            '                      AND b.bdr_type_id  = :l_BDR_Type ' || CHR(10) ||      
            '                      AND b.trf_type IN (' || pk114_items.Get_List_BDR_Types(p_Data_Type,'B','D') || ')' || CHR(10) ||
            '                      AND t.order_id_b IS NOT NULL ' || CHR(10) ||     
            '                      AND t.cdr_id = b.cdr_id ' || CHR(10) ||     
            '                      AND (b.bill_id < 0 OR b.bill_id IS NULL ' ||
                                     '   OR ' ||
                                     '  EXISTS (SELECT 1 ' || CHR(10) ||
                                     '            FROM BILL_T bl ' || CHR(10) ||
                                     '           WHERE bl.rep_period_id = :l_Rep_Period_Id ' || CHR(10) || 
                                     '             AND bl.bill_status IN (:l_Open) ' || CHR(10) ||
                                     '             AND bl.bill_id = b.bill_id) ' || CHR(10) ||
                                     ' ) ' || CHR(10) ||
            '      ) '                                         
        USING pk00_const.c_ITEM_STATE_ERROR,
              l_Rep_Period_Id,                
              pk00_const.c_ITEM_STATE_OPEN,
              NVL(l_Rep_Period, p_Date_From), NVL(l_Rep_Period, p_Date_To),
              l_BDR_Type_Id,
              l_Rep_Period_Id,
              pk00_const.c_BILL_STATE_OPEN;
       
       -- ������� BDR-�
        EXECUTE IMMEDIATE
            'DELETE /*++ parallel(b 5) */ FROM ' || l_BDR_Table || ' b ' || CHR(10) ||
            ' WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
            '   AND b.bdr_type_id = :l_BDR_Type ' || CHR(10) ||
            '   AND b.trf_type IN (' || pk114_items.Get_List_BDR_Types(p_Data_Type,'B') || ')' ||                        
            '   AND (b.bill_id < 0 OR b.bill_id IS NULL ' ||
                    '  OR ' ||
                    ' EXISTS (SELECT 1 ' || CHR(10) ||
                    '           FROM BILL_T bl ' || CHR(10) ||
                    '          WHERE bl.rep_period_id = :l_Rep_Period_Id ' || CHR(10) || 
                    '            AND bl.bill_status IN (:l_Open) ' || CHR(10) ||
                    '            AND bl.bill_id = b.bill_id) ' || CHR(10) ||
            '      ) ' || CHR(10) ||        
            '   AND EXISTS (SELECT 1 ' || CHR(10) ||
            '                  FROM '|| p_Data_Table || ' t '|| CHR(10) ||
            '                 WHERE t.order_id_b IS NOT NULL ' || CHR(10) ||   
            '                   AND t.cdr_id = b.cdr_id) '
        USING NVL(l_Rep_Period, p_Date_From), NVL(l_Rep_Period, p_Date_To),
              l_BDR_Type_Id, 
              l_Rep_Period_Id,
              pk00_const.c_BILL_STATE_OPEN;
        
        l_Count := l_Count + SQL%ROWCOUNT;
    
    END IF;
    
   -- ��������� ����� BDR-� � �������
    EXECUTE IMMEDIATE
        'INSERT INTO ' || NVL(p_Test_BDR_Table,l_BDR_Table) ||
        ' SELECT /*++ parallel(t 5) */ * ' ||
        '   FROM BDR_OPER_TMP t';
            
    l_InsBDRCnt := SQL%ROWCOUNT;        
      
    --
    -- -- +++++++++++++++++++++++++++++++++++++++++++


    Pk01_Syslog.Write_Msg(p_Msg => '�K: ' || TO_CHAR(p_Date_From,'dd.mm.yyyy') || ' - ' || TO_CHAR(p_Date_To,'dd.mm.yyyy') ||
                                   ', Src: ' || p_Data_Type ||
                                   '; Ins. '|| TO_CHAR(l_InsBDRCnt) || ' (to: ' || l_BDR_Table || ') ' ||
                                   ', Dlt. old bdr ' || TO_CHAR(l_Count) || 
                                   '; Task_Id: ' || TO_CHAR(p_Task_Id), 
                          p_Src => gc_PkgName || '.' || v_prcName);    
    COMMIT;
    
    IF p_Test_BDR_Table IS NULL THEN
        mdv.pk21_lock.UnLock_Resource(p_Lock_Name => l_Lock_Name);
    END IF;                                 
                                             
/*    
EXCEPTION
    WHEN ERR_BILL_PARAM THEN
          Pk01_Syslog.Write_to_log(p_Msg => '������. � �������� ������� �������� p_Bill �� ����� ���� FALSE.', 
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
END Load_BDR_XTTK;

PROCEDURE Recalc_Op_V_Tariff(p_Data_Type   IN varchar2,  
                             p_Rep_Period  IN date,
                             p_Modify_Date IN date DEFAULT NULL)
IS

    v_prcName    CONSTANT varchar2(20) := 'Recalc_Op_V_Tariff';

    l_Date_From  date;
    l_Date_To    date;

    l_BDR_Table  varchar2(32);
    l_BDR_Source number;
    
    l_Agent      number;
    l_Items      number;
    
BEGIN

    l_Date_From := TRUNC(p_Rep_Period,'MM');
    l_Date_To   := LAST_DAY(TRUNC(p_Rep_Period,'MM')) + INTERVAL '00 23:59:59' DAY TO SECOND;

   -- �������� ��� ������� BDR-��, ������������ ��� ������� ���� �������� �
   -- ��� bdr-��, ��� ������� ����� ������� ������������������� ������
    l_BDR_Source := PIN.Get_BDR_Type(p_Data_Type => p_Data_Type,
                                     p_BDR_Table => l_BDR_Table,
                                     p_Agent     => l_Agent,
                                     p_Items     => l_Items  
                                    );       

   -- ������������� �������� ������
--    EXECUTE IMMEDIATE
--        ' MERGE /*+ parallel(b 10) */ INTO ' || l_BDR_Table || ' b ' || CHR(10) ||
/*        ' USING ( ' || CHR(10) ||
        '        SELECT order_id, MONTH, op_rate_plan_id, subservice_id, rec_id, price ' || CHR(10) ||
        '          FROM ( ' || CHR(10) ||
        '                SELECT b.order_id, b.MONTH, b.op_rate_plan_id, b.subservice_id, b.price_id, p.rec_id, p.price, ' || CHR(10) || 
        '                       row_number() OVER (PARTITION BY b.op_rate_plan_id, b.subservice_id, b.price_id ' || CHR(10) ||
        '                                              ORDER BY p.vol DESC) rn ' || CHR(10) ||
        '                  FROM ( ' || CHR(10) || */
--        '                        SELECT /*+ parallel(b 10) */ order_id, TRUNC(b.start_time,''mm'') MONTH, op_rate_plan_id, subservice_id, NVL(price_id,-1) price_id, ' || CHR(10) ||
/*        '                               SUM(NVL(duration,0))/60 mins ' || CHR(10) ||
        '                          FROM ' || l_BDR_Table || ' b ' || CHR(10) ||
        '                         WHERE rep_period BETWEEN :l_date_from AND :l_date_to ' || CHR(10) || 
        '                           AND vol_type = :l_Vol_Type ' || CHR(10) ||-- 1 
        '                         GROUP BY order_id, op_rate_plan_id, subservice_id, NVL(price_id,-1), TRUNC(b.start_time,''mm'') ' || CHR(10) ||
        '                       ) b, ' || CHR(10) ||
        '                       x07_ord_price_v_t p ' || CHR(10) ||
        '                 WHERE b.op_rate_plan_id = p.op_rate_plan_id ' || CHR(10) ||
        '                   AND b.subservice_id   = p.subservice_id ' || CHR(10) ||
        '                   AND p.vol <= b.mins ' || CHR(10) ||
        '                 ORDER BY p.vol DESC ' || CHR(10) ||
        '               ) ' || CHR(10) ||
        '         WHERE rn = 1 ' || CHR(10) ||
        '           AND price_id != rec_id ' || CHR(10) || -- ��������� �������� ����������
        '       ) p ' || CHR(10) ||        
        '   ON (b.rep_period BETWEEN :l_date_from AND :l_date_to ' || CHR(10) || 
        '       AND b.vol_type = 1 ' || CHR(10) ||
        '       AND b.order_id = p.order_id ' || CHR(10) ||
        '       AND TRUNC(b.start_time,''mm'') = p.MONTH ' || CHR(10) || 
        '       AND b.op_rate_plan_id = p.op_rate_plan_id ' || CHR(10) ||
        '       AND b.subservice_id = p.subservice_id ' || CHR(10) ||
        '      ) ' || CHR(10) ||
        ' WHEN MATCHED THEN UPDATE ' || CHR(10) ||                   
        '  SET  b.tariff = p.price, ' || CHR(10) ||
        '       b.price_id = p.rec_id, ' || CHR(10) ||
        '       b.amount = b.duration/60*p.price, ' || CHR(10) ||
        '       b.bdr_status = :l_OK, ' || CHR(10) ||
        '       b.modify_date = NVL(:p_Modify_Date, SYSDATE)'
    USING l_Date_From, l_Date_To,
          pin.pk00_const.c_TARIF_VOL_TYPE_VOL,
          l_Date_From, l_Date_To,
          pin.pk00_const.c_RET_OK,
          p_Modify_Date;  */


    INSERT INTO TMP13_OP_V_TARIFF
           (order_id, calc_month, op_rate_plan_id, srv_id, rec_id, price)
    SELECT order_id, calc_month, op_rate_plan_id, subservice_id, rec_id, price 
      FROM ( 
            SELECT b.order_id, b.calc_month, b.op_rate_plan_id, b.subservice_id, b.price_id, p.rec_id, p.price,  
                   row_number() OVER (PARTITION BY b.op_rate_plan_id, b.subservice_id, b.price_id 
                                          ORDER BY p.vol DESC) rn 
              FROM ( 
                    SELECT /*+ parallel(b 5) */
                           order_id, TRUNC(b.local_time,'mm') calc_month,  
                           op_rate_plan_id, subservice_id, NVL(price_id,-1) price_id, 
                           SUM(NVL(duration,0))/60 mins 
                      FROM BDR_OPER_T b 
                     WHERE rep_period BETWEEN l_date_from AND l_date_to  
                       AND vol_type = pin.pk00_const.c_TARIF_VOL_TYPE_VOL --:l_Vol_Type -- 1 
                     GROUP BY order_id, op_rate_plan_id, subservice_id, NVL(price_id,-1), TRUNC(b.local_time,'mm') 
                   ) b, 
                   x07_ord_price_v_t p 
             WHERE b.op_rate_plan_id = p.op_rate_plan_id 
               AND b.subservice_id   = p.srv_id 
               AND p.vol <= b.mins 
             ORDER BY p.vol DESC 
           ) 
     WHERE rn = 1 
       AND price_id != rec_id;

    MERGE /*+ parallel(b 10) */ INTO BDR_OPER_T b 
         USING (SELECT order_id, calc_month, op_rate_plan_id, srv_id, rec_id, price
                  FROM TMP13_OP_V_TARIFF 
               ) p         
           ON (b.rep_period BETWEEN l_date_from AND l_date_to 
               AND b.vol_type = pin.pk00_const.c_TARIF_VOL_TYPE_VOL 
               AND b.order_id = p.order_id
               AND TRUNC(b.local_time,'mm') = p.calc_month  
               AND b.op_rate_plan_id = p.op_rate_plan_id 
               AND b.subservice_id = p.srv_id 
              ) 
         WHEN MATCHED THEN UPDATE                    
          SET  b.tariff = p.price,
               b.price_id = p.rec_id,
               b.amount = b.duration/60*p.price,
               b.bdr_status = pin.pk00_const.c_RET_OK,
               b.modify_date = NVL(p_Modify_Date, SYSDATE);


    Pk01_Syslog.Write_Msg(p_Msg => '������: ' || TO_CHAR(l_Date_From,'dd.mm.yyyy') ||
                                   '. ����������� BDR-�� � ��������� ��������: ' || 
                                   TO_CHAR(SQL%ROWCOUNT), 
                          p_Src => gc_PkgName || '.' || v_prcName);    

END Recalc_Op_V_Tariff;             



-- ���� ������� ��������� ����������� ������ ����� ��������� DEF-�� � ���� �������,
-- �� ���������� ������� ����. �����. ���� �� ������� - NULL
FUNCTION Get_Def(p_Ph_Num  IN varchar2,
                 p_Sw_Name IN varchar2
                ) RETURN varchar2 
IS
    l_Prefix varchar2(16);
    l_Def    number;
BEGIN

    -- ��������� ����� � DEF ��� ��������� ���� ������ ��� ���
    SELECT prefix
      INTO l_Prefix
      FROM (SELECT d.prefix
              FROM pin.switch_t s,
                   tariff_ph.D02_ZONE_ABC a,
                   tariff_ph.d01_zone z,
                   tariff_ph.D03_ZONE_DEF d
             WHERE s.switch_code = p_Sw_Name 
               AND s.local_prefix = a.prefix
               AND A.ABC_H_ID = z.abc_h_id
               AND z.DEF_H_ID = d.def_h_id
               AND p_Ph_Num LIKE d.prefix || '%'
             ORDER BY LENGTH(d.prefix) DESC
           )  
     WHERE ROWNUM = 1;
     
    RETURN l_Prefix; 
     
EXCEPTION
    WHEN no_data_found THEN
        RETURN NULL;       
END Get_Def;                               

-- ��������� ��������������� ������� 
FUNCTION Get_Bill_Duration(p_Seconds         IN number,
                           p_Op_Rate_Plan_Id IN number,
                           p_Round_V_Id      IN number DEFAULT NULL
                          ) RETURN number
IS

    l_Result number;

BEGIN

   -- ��� ���� ��������� ���������� �����������
    IF p_Round_V_Id IS NULL THEN
       -- ������� ���������� �� ������, ���� ��� �� id ���. �����
        SELECT r.round_v_id
          INTO l_Result
          FROM x07_op_rate_plan r
         WHERE r.Op_Rate_Plan_Id = p_Op_Rate_Plan_Id;
          
    END IF;           
         
    IF p_Round_V_Id = 1 OR l_Result = 1 THEN
        l_Result := CEIL(p_Seconds/60); -- �� ��������� ������� ������
    ELSE
        RAISE no_data_found; -- ���������� �����������
    END IF;         
         
    RETURN l_Result;
               
EXCEPTION
    WHEN no_data_found THEN
       -- ���������� �����������    
        RETURN p_Seconds/60;

END Get_Bill_Duration;      

-- ����� ID ��������� ����� ��� �������  
FUNCTION Get_XTTK_R_RP_ID_Old(p_OP_Sw_Id    IN  number,
                          p_Phone       IN  varchar2,
                          p_Date        IN  date,
                          p_RP_Type     IN  number,
                          p_RatePlan_Id IN  number,
                          p_Trf_Vol     OUT number,
                          p_SubSrv_Id   OUT number,
                          p_Op_Rp_Id    OUT number
                         ) RETURN number
IS

BEGIN


/*    SELECT op_rate_plan_id, tarif_vol_type, srv_id
      INTO  p_Op_Rp_Id, p_Trf_Vol, p_SubSrv_Id
      FROM (SELECT s.op_rate_plan_id, r.tarif_vol_type, s.srv_id,
                   row_number() OVER (ORDER BY s.op_sw_id NULLS LAST, (TO_NUMBER(s.phone_to)-TO_NUMBER(s.phone_from)) ASC) rn -- ����� dense_rank ��� �� ������ ������� �����������
              FROM RSX07_ORD_SERVICE_R_TM s,
                   X07_OP_RATE_PLAN r
             WHERE s.op_rate_plan_id = r.op_rate_plan_id
               AND r.op_rate_plan_type = p_RP_Type
               AND r.rateplan_id = p_RatePlan_Id
               AND (
                    s.op_sw_id = p_Op_Sw_Id
                     OR
                    s.op_sw_id IS NULL
                   )               
               AND TO_CHAR(s.mask_value) = SUBSTR(p_Phone, 1, pin.pk00_const.c_Mask_Length)
               AND p_Phone BETWEEN s.phone_from AND s.phone_to
               AND p_Date BETWEEN s.date_from AND NVL(s.date_to, gc_MaxDate)
           )
     WHERE rn = 1;  */
     
    SELECT op_rate_plan_id, tarif_vol_type, srv_id
      INTO  p_Op_Rp_Id, p_Trf_Vol, p_SubSrv_Id
      FROM (SELECT s.op_rate_plan_id, r.tarif_vol_type, s.srv_id,
                   row_number() OVER (ORDER BY s.op_sw_id NULLS LAST, (TO_NUMBER(s.phone_to)-TO_NUMBER(s.phone_from)) ASC) rn -- ����� dense_rank ��� �� ������ ������� �����������
              FROM X07_ORD_SERVICE_R_T s,
                   X07_OP_RATE_PLAN r
             WHERE s.op_rate_plan_id = r.op_rate_plan_id
               AND r.op_rate_plan_type = p_RP_Type
               AND r.rateplan_id = p_RatePlan_Id
               AND (
                    s.op_sw_id = p_Op_Sw_Id
                     OR
                    s.op_sw_id IS NULL
                   )               
               AND p_Phone BETWEEN s.phone_from AND s.phone_to
               AND p_Date BETWEEN s.date_from AND NVL(s.date_to, gc_MaxDate)
           )
     WHERE rn = 1;          

    RETURN pin.pk00_const.c_RET_OK;

EXCEPTION
    WHEN no_data_found THEN
        RETURN pin.pk00_const.c_TARIFF_NOT_FOUND;
        
/*    WHEN others THEN
        Pk01_Syslog.Write_Msg(p_Msg => 'D�te: ' || TO_CHAR(p_Date,'dd.mm.yyyy') ||
                                       '; Phone '|| TO_CHAR(p_Phone) ||  
                                       '; Op_Sw_Id: ' || TO_CHAR(p_Op_Sw_Id) ||
                                       '; RP_Id: ' || TO_CHAR(p_RatePlan_Id) ||
                                       '; RP_Type: ' || TO_CHAR(p_RP_Type),
                              p_Src => gc_PkgName || '.Get_SPB_R_RP_ID');        
     raise;     */  
END Get_XTTK_R_RP_ID_Old;

-- ����� ID ��������� ����� ��� ������  
FUNCTION Get_XTTK_D_RP_ID_Old(p_Switch_Id   IN  number,
                          p_Phone       IN  varchar2,
                          p_Date        IN  date,
                          p_RP_Type     IN  number,
                          p_RatePlan_Id IN  number,
                          p_Trf_Vol     OUT number,
                          p_SubSrv_Id   OUT number,
                          p_Op_Rp_Id    OUT number
                         ) RETURN number
IS

BEGIN

 /*   SELECT op_rate_plan_id, tarif_vol_type, srv_id
      INTO p_Op_Rp_Id, p_Trf_Vol, p_SubSrv_Id
      FROM (SELECT s.op_rate_plan_id, r.tarif_vol_type, s.srv_id,
                   DENSE_RANK() OVER (ORDER BY s.switch_id NULLS LAST, 
                                               (TO_NUMBER(s.phone_to)-TO_NUMBER(s.phone_from))) dr -- ����� dense_rank ��� �� ������ ������� �����������
              FROM RSX07_ORD_SERVICE_D_TM s,
                   X07_OP_RATE_PLAN r
             WHERE s.op_rate_plan_id = r.op_rate_plan_id
               AND r.op_rate_plan_type = p_RP_Type
               AND r.rateplan_id = p_RatePlan_Id
               AND (s.switch_id = p_Switch_Id OR s.switch_id IS NULL)               
               AND TO_CHAR(s.mask_value) = SUBSTR(p_Phone, 1, pin.pk00_const.c_Mask_Length)
               AND p_Phone BETWEEN s.phone_from AND s.phone_to
               AND p_Date BETWEEN s.date_from AND NVL(s.date_to, gc_MaxDate)
             ORDER BY s.date_from DESC
            )
      WHERE dr = 1;        */
      
    SELECT op_rate_plan_id, tarif_vol_type, srv_id
      INTO p_Op_Rp_Id, p_Trf_Vol, p_SubSrv_Id
      FROM (SELECT s.op_rate_plan_id, r.tarif_vol_type, s.srv_id,
                   DENSE_RANK() OVER (ORDER BY s.switch_id NULLS LAST, 
                                               (TO_NUMBER(s.phone_to)-TO_NUMBER(s.phone_from))) dr -- ����� dense_rank ��� �� ������ ������� �����������
              FROM X07_ORD_SERVICE_D_T s,
                   X07_OP_RATE_PLAN r
             WHERE s.op_rate_plan_id = r.op_rate_plan_id
               AND r.op_rate_plan_type = p_RP_Type
               AND r.rateplan_id = p_RatePlan_Id
               AND (s.switch_id = p_Switch_Id OR s.switch_id IS NULL)               
               AND p_Phone BETWEEN s.phone_from AND s.phone_to
               AND p_Date BETWEEN s.date_from AND NVL(s.date_to, gc_MaxDate)
             ORDER BY s.date_from DESC
            )
      WHERE dr = 1;        
        
    RETURN pin.pk00_const.c_RET_OK;   
       
EXCEPTION
    WHEN no_data_found THEN
        RETURN pin.pk00_const.c_TARIFF_NOT_FOUND;    
END Get_XTTK_D_RP_ID_Old;


-- ����� ID ��������� ����� 
FUNCTION Get_XTTK_RP_ID(p_RP_Type     IN  number,
                        p_RatePlan_Id IN  number,
                        p_Trf_Vol     OUT number,
                        p_Op_Rp_Id    OUT number
                       ) RETURN number
IS

BEGIN

    SELECT r.op_rate_plan_id, r.tarif_vol_type
      INTO p_Op_Rp_Id, p_Trf_Vol
      FROM X07_OP_RATE_PLAN r
     WHERE r.op_rate_plan_type = p_RP_Type
       AND r.rateplan_id = p_RatePlan_Id;

    RETURN pin.pk00_const.c_RET_OK;

EXCEPTION
    WHEN no_data_found THEN
        RETURN pin.pk00_const.c_TARIFF_NOT_FOUND;
        
 /*   WHEN others THEN
        Pk01_Syslog.Write_Msg(p_Msg => 'D�te: ' || TO_CHAR(p_Date,'dd.mm.yyyy') ||
                                       '; Phone '|| TO_CHAR(p_Phone) ||  
                                       '; Op_Sw_Id: ' || TO_CHAR(p_Op_Sw_Id) ||
                                       '; RP_Id: ' || TO_CHAR(p_RatePlan_Id) ||
                                       '; RP_Type: ' || TO_CHAR(p_RP_Type),
                              p_Src => gc_PkgName || '.Get_SPB_R_RP_ID');  */      
        
END Get_XTTK_RP_ID;

-- �� �������� �������� �� ��������� ��
FUNCTION Get_XTTK_RP_Price(p_Op_RP_Id  IN  number,
                           p_SubSrv_Id IN  number,
                           p_Date      IN  date,
                           p_Price     OUT number,
                           p_Price_Id  OUT number
                          ) RETURN number
IS

BEGIN

    SELECT p.price, p.rec_id
      INTO p_Price, p_Price_Id
      FROM X07_ORD_PRICE_T p
     WHERE p.op_rate_plan_id = p_Op_RP_Id
       AND p.srv_id = p_SubSrv_Id
       AND p_Date BETWEEN p.date_from AND NVL(p.date_to, gc_MaxDate);   

    RETURN pin.pk00_const.c_RET_OK;

EXCEPTION
    WHEN no_data_found THEN
        RETURN pin.pk00_const.c_PRICE_NOT_FOUND;
END Get_XTTK_RP_Price;


-- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
-- ����� ID ��������� ����� ��� �������  
FUNCTION Get_XTTK_R_RP_ID(p_Op_Rate_Plan_Id IN  number,
                          p_OP_Sw_Id        IN  number,
                          p_Phone           IN  varchar2,
                          p_Date            IN  date,
                          p_SubSrv_Id       OUT number
                         ) RETURN number
IS

BEGIN

    SELECT srv_id
      INTO p_SubSrv_Id
      FROM (SELECT s.op_rate_plan_id, s.srv_id,
                   row_number() OVER (ORDER BY s.op_sw_id NULLS LAST, (TO_NUMBER(s.phone_to)-TO_NUMBER(s.phone_from)) ASC) rn -- ����� dense_rank ��� �� ������ ������� �����������
              FROM X07_ORD_SERVICE_R_T s
             WHERE s.op_rate_plan_id = p_Op_Rate_Plan_Id
               AND (
                    s.op_sw_id = p_Op_Sw_Id
                     OR
                    s.op_sw_id IS NULL
                   )               
               AND p_Phone BETWEEN s.phone_from AND s.phone_to
               AND p_Date BETWEEN s.date_from AND NVL(s.date_to, gc_MaxDate)
           )
     WHERE rn = 1;          

    RETURN pin.pk00_const.c_RET_OK;

EXCEPTION
    WHEN no_data_found THEN
        RETURN pin.pk00_const.c_TARIFF_NOT_FOUND;
        
/*    WHEN others THEN
        Pk01_Syslog.Write_Msg(p_Msg => 'D�te: ' || TO_CHAR(p_Date,'dd.mm.yyyy') ||
                                       '; Phone '|| TO_CHAR(p_Phone) ||  
                                       '; Op_Sw_Id: ' || TO_CHAR(p_Op_Sw_Id) ||
                                       '; RP_Id: ' || TO_CHAR(p_RatePlan_Id) ||
                                       '; RP_Type: ' || TO_CHAR(p_RP_Type),
                              p_Src => gc_PkgName || '.Get_SPB_R_RP_ID');        
     raise;     */  
END Get_XTTK_R_RP_ID;

-- ����� ID ��������� ����� ��� ������  
FUNCTION Get_XTTK_D_RP_ID(p_Op_Rate_Plan_Id IN  number,
                          p_Switch_Id       IN  number,
                          p_Phone           IN  varchar2,
                          p_Date            IN  date,
                          p_SubSrv_Id       OUT number
                         ) RETURN number
IS

BEGIN

    SELECT srv_id
      INTO p_SubSrv_Id
      FROM (SELECT 
                   s.op_rate_plan_id, s.srv_id,
                   row_number() OVER (ORDER BY s.switch_id NULLS LAST, 
                                               (TO_NUMBER(s.phone_to)-TO_NUMBER(s.phone_from))) rn -- ����� dense_rank ��� �� ������ ������� �����������
              FROM X07_ORD_SERVICE_D_T s
             WHERE s.op_rate_plan_id = p_Op_Rate_Plan_Id
               AND (s.switch_id = p_Switch_Id OR s.switch_id IS NULL)               
               AND p_Phone BETWEEN s.phone_from AND s.phone_to
               AND p_Date BETWEEN s.date_from AND NVL(s.date_to, gc_MaxDate)
             ORDER BY s.date_from DESC
            )
      WHERE rn = 1;        
        
    RETURN pin.pk00_const.c_RET_OK;   
       
EXCEPTION
    WHEN no_data_found THEN
        RETURN pin.pk00_const.c_TARIFF_NOT_FOUND;    
END Get_XTTK_D_RP_ID;


FUNCTION Get_Direction_New(p_Ph_Number IN  varchar2,
                           p_Dir_Name  OUT varchar2
                          ) RETURN number
IS
    l_Length PLS_INTEGER;
    l_Result number;
BEGIN

    l_Length := gl_Max_Prefix;
    
    LOOP
        
        BEGIN
        
            p_Dir_Name := t_Prefix(SUBSTR(p_Ph_Number, 1, l_Length));
        
            l_Result := SUBSTR(p_Ph_Number, 1, l_Length);
        
        EXCEPTION
            WHEN no_data_found THEN
                l_Length := l_Length - 1;
            WHEN others THEN
                IF SQLCODE = -6502 THEN    
                    l_Length := 0; -- �������, �.�. �������� ����� = NULL
                ELSE
                    RAISE;    
                END IF;     
        END;
        
        EXIT WHEN l_Length = 0 OR l_Result IS NOT NULL;
        
    END LOOP;
    
    RETURN l_Result;
    
END Get_Direction_New; 

FUNCTION Trf_Oper_Table(pr_Call     ref_Trf_Op,
                        p_Period_Id number,
                        p_Task_Id   number
                       )
                      RETURN PIN.OP_BDR_COLL
                      PIPELINED PARALLEL_ENABLE (PARTITION pr_Call BY ANY)                             
IS

    l_Counter       PLS_INTEGER;

    l_cur           pr_Call%ROWTYPE;
    ret_rec         PIN.OP_BDR_TYPE := PIN.OP_BDR_TYPE(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                                       NULL,NULL);

    l_Op_Sw_Id      number;
    l_Trf_Step      PLS_INTEGER;
    l_Order_Step    PLS_INTEGER;
    l_Prefix        varchar2(16);
    l_TG_Tariff     BOOLEAN;
    l_Order_Ph      varchar2(32);
    l_Tg            varchar2(16);
    l_Side          varchar2(1);
    l_Network_Code  varchar2(16);
    l_Sw_Id         NUMBER;
    l_Round_V_Id    number;
    l_BDR_Code      varchar2(32);
    l_Loc_Call      number;
                         
   -- ��������� � �������������������� BDR-���
    TYPE t_Ret_Rec IS TABLE OF PIN.OP_BDR_TYPE INDEX BY PLS_INTEGER;
    -- ��������� � �������������������� BDR-��� ��� ���� ������� 
    -- (��������� ������� �� ����� � ��� �� ��������)
    TYPE t_Ret_Colls IS TABLE OF t_Ret_Rec INDEX BY PLS_INTEGER;
    t_ret_col t_Ret_Colls;
                               
    -- ��� ����������� ���. ������
    TYPE r_OP_RP IS RECORD(tariff_vol_type number,
                           op_rate_plan_id number,
                           round_v_id      number
                          ); 
    TYPE t_OP_RP_Type IS TABLE OF r_OP_RP INDEX BY PLS_INTEGER;
    TYPE t_Rate_Plan IS TABLE OF t_OP_RP_Type INDEX BY PLS_INTEGER;    
    
    lt_Rate_Plan t_Rate_Plan;

   -- ��� �������� ��� ��������� ������
    t_Bill t_NumV;  

   -- ��� �������� ������ �������, ������� ���� ������������
    TYPE t_Trf_Type IS TABLE OF number;
    lt_Trf_Type t_Trf_Type;

    
    PROCEDURE Set_Counter
    IS
    BEGIN
    
        l_Counter := l_Counter + 1;
        IF MOD(l_Counter, 500) = 0 THEN
            DBMS_APPLICATION_INFO.SET_ACTION(TO_CHAR(l_cur.start_time,'dd.mm.yyyy') || ' SCAN ROWS: ' || TO_CHAR(l_Counter));
        END IF;
            
    END Set_Counter;      

   -- ������������� ������ �������, ������� ���� ������������ ��� ����������� 
    PROCEDURE Init_Trf_Types(p_Data_Type IN varchar2,
                             p_Side      IN varchar2)
    IS
        lt_Load_Type NUM_COLL;
        l_Index      PLS_INTEGER;
        l_Trf_Types  varchar2(32);
    BEGIN
       -- ���� ������ ������� �� ���������������, �� �������� �����
       -- ��� ��� ������
        IF p_Task_Id > 0 THEN
            BEGIN    
                SELECT q.opr_trf_type
                  INTO l_Trf_Types
                  FROM Q00_RETRF_JOB q
                 WHERE q.task_id = p_Task_Id; 
                
            EXCEPTION
                WHEN no_data_found THEN    
                    l_Trf_Types := NULL;
            END;
            
        END IF;
        
        -- ���� ������ �� ����������, �� �������� ��� ��������� ���� 
        IF l_Trf_Types IS NULL THEN
        
            l_Trf_Types := pk114_items.Get_List_BDR_Types(p_Data_Type => p_Data_Type,
                                                          p_Side      => p_Side,
                                                          p_In_Out    => NULL
                                                         ); 
        
        END IF;
                     
      -- ��������� ��� �������� � ��������� ��������� ����� ������� 
        EXECUTE IMMEDIATE 'BEGIN :t := NUM_COLL(' || l_Trf_Types || '); END;'
          USING OUT lt_Load_Type;

        lt_Trf_Type.DELETE;

        l_Index := 1;

        FOR idx IN lt_Load_Type.FIRST .. lt_Load_Type.LAST
        LOOP
            
            IF (p_Side = 'A' AND lt_Load_Type(idx) IN (pin.pk00_const.c_OP_RATE_PLAN_TYPE_RIP,
                                                       pin.pk00_const.c_OP_RATE_PLAN_TYPE_DT,
                                                       pin.pk00_const.c_OP_RATE_PLAN_TYPE_RI)
               )
               OR
              (p_Side = 'B' AND lt_Load_Type(idx) IN (pin.pk00_const.c_OP_RATE_PLAN_TYPE_DIP,
                                                      pin.pk00_const.c_OP_RATE_PLAN_TYPE_RT,
                                                      pin.pk00_const.c_OP_RATE_PLAN_TYPE_DI)
               )                                         
            THEN
            
                lt_Trf_Type.EXTEND;
            
                IF lt_Load_Type(idx) IN (pin.pk00_const.c_OP_RATE_PLAN_TYPE_RIP, pin.pk00_const.c_OP_RATE_PLAN_TYPE_DIP)
                THEN
                   -- ������ �� ������ �����, �.�. ��� ������ ��������� �������
                    IF l_Index != 1 THEN
                        -- ��������� ��, ��� ���� �� ������ ����� � �����
                        lt_Trf_Type(l_Index) := lt_Trf_Type(1);         
                    
                    END IF;
                
                    lt_Trf_Type(1) := lt_Load_Type(idx);
                
                ELSE
                
                    lt_Trf_Type(l_Index) := lt_Load_Type(idx);
                
                END IF;
     
                l_Index := l_Index + 1;

            END IF;
        
        END LOOP;

    END Init_Trf_Types;

BEGIN
    
    l_Counter := 0;
    
    -- ������������� ������� � ��������� �������
    FOR l_cur IN (SELECT rateplan_id, op_rate_plan_type, op_rate_plan_id, tarif_vol_type, round_v_id
                    FROM x07_op_rate_plan)
    LOOP
        lt_Rate_Plan(l_cur.rateplan_id)(l_cur.op_rate_plan_type).tariff_vol_type := l_cur.tarif_vol_type;
        lt_Rate_Plan(l_cur.rateplan_id)(l_cur.op_rate_plan_type).op_rate_plan_id := l_cur.op_rate_plan_id;
        lt_Rate_Plan(l_cur.rateplan_id)(l_cur.op_rate_plan_type).round_v_id      := l_cur.round_v_id; 
    
    END LOOP;                    
    
    -- ������������� ������� ��� ������ ������
    lt_Trf_Type := t_Trf_Type();
    
    FETCH pr_Call INTO l_cur;
    
    LOOP
        EXIT WHEN pr_Call%NOTFOUND;    
    
        l_Side := NULL;
        
       -- �������� id �����������
        l_Sw_Id := t_Switch(l_cur.sw_name);   
        
       -- �������� ����
        l_Network_Code := t_BDR_Network(l_cur.bdr_type_id).network_code; 
        l_BDR_Code     := t_BDR_Network(l_cur.bdr_type_id).bdr_code;
        
        LOOP -- ������� �����������. ������� ����������� ������� �, � ����� � (���� ���������)

            ret_rec := PIN.OP_BDR_TYPE(NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                       NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
                                       NULL,NULL);
                                 
            l_TG_Tariff  := FALSE; 
            l_Round_V_Id := NULL;
            
            ret_rec.row_id          := ROWIDTOCHAR(l_cur.row_id);
            ret_rec.bdr_type_id     := l_cur.bdr_type_id;
            ret_rec.start_time      := l_cur.Start_Time; 
            ret_rec.Cdr_Id          := l_cur.cdr_id;        
            ret_rec.Duration        := l_cur.duration;        
            ret_rec.sw_name         := l_cur.sw_name;
            ret_rec.tg_in           := l_cur.tg_in;
            ret_rec.tg_out          := l_cur.tg_out;

           -- �������� ���������� ������ � ������������ ����
            ret_rec.abn_a := pin.Norm_XTTK_Ph_Num(p_Sw_Code => ret_rec.sw_name,
                                                  p_PH_Num  => l_cur.Abn_A);       
           
            ret_rec.abn_b := pin.Norm_XTTK_Ph_Num(p_Sw_Code => ret_rec.sw_name,
                                                  p_PH_Num  => l_cur.Abn_B);       

           -- �������� ��� �����������
            ret_rec.pref_b := Get_Direction_New(p_Ph_Number => ret_rec.abn_b,
                                                p_Dir_Name  => ret_rec.term_z_name
                                               );            

           -- ���������� ������� ��� ��� �����
            IF LENGTH(ret_rec.Abn_B) < 11 THEN
                l_Loc_Call := 1; -- ����� �������
                
            ELSE
                BEGIN
                
                    SELECT 1 -- ����� �������
                      INTO l_Loc_Call
                      FROM switch_t s
                     WHERE s.switch_code = ret_rec.sw_name
                       AND ret_rec.Abn_B LIKE s.local_prefix || '%';
                
                EXCEPTION
                    WHEN no_data_found THEN
                        l_Loc_Call := 0; 
                END;    
                
            END IF;        
           

            IF (l_cur.order_id_a > 0 OR l_cur.order_id_a = pin.pk00_const.c_TOO_MANY_ORDERS) 
                 AND 
               l_Side IS NULL 
            THEN
            
                l_Side                := 'A';
                ret_rec.UTC_Offset    := l_cur.utc_offset_a;         
                ret_rec.Bill_Date     := l_cur.local_time_a;
                ret_rec.Order_Id      := l_cur.order_id_a;            
                ret_rec.order_swtg_id := l_cur.order_a_swtg_id;   
                l_Tg                  := l_cur.tg_in;
                l_Order_Ph            := l_cur.order_ph_a;

            ELSIF (l_cur.order_id_b > 0 OR l_cur.order_id_b = pin.pk00_const.c_TOO_MANY_ORDERS) 
                    AND 
                  (l_Side IS NULL OR l_Side = 'A') 
            THEN
            
                l_Side                := 'B';
                ret_rec.UTC_Offset    := l_cur.utc_offset_b;         
                ret_rec.Bill_Date     := l_cur.local_time_b;
                ret_rec.Order_Id      := l_cur.order_id_b;    
                ret_rec.order_swtg_id := l_cur.order_b_swtg_id;
                l_Tg                  := l_cur.tg_out;  
                l_Order_Ph            := l_cur.order_ph_b;      
                        
            ELSE
                EXIT; -- ��� ��������� ��� ������� ������, ������� �� �����
            END IF;    

          -- ������������� ������� � ������� ������ ��� ������� �������, ������� ���� ������������ 
            Init_Trf_Types(p_Data_Type => l_BDR_Code,
                           p_Side      => l_Side);

            l_Order_Step := 0; -- ������� ������� �� ����� � ��� �� ��������

            t_ret_col.DELETE;

            LOOP 
            
                ret_rec.vol_type         := NULL;
                ret_rec.op_rate_plan_id  := NULL;
                ret_rec.subservice_id    := NULL;
                ret_rec.service_id       := NULL;
                ret_rec.parent_subsrv_id := NULL;
                ret_rec.price            := NULL;  
                ret_rec.price_id         := NULL;
                ret_rec.Amount           := NULL;        
        
                IF (
                    (
                     (l_Side = 'B' AND l_cur.order_id_b = pin.pk00_const.c_TOO_MANY_ORDERS)
                       OR
                     (l_Side = 'A' AND l_cur.order_id_a = pin.pk00_const.c_TOO_MANY_ORDERS)      
                    )           
                    AND l_Network_Code = 'SPB'
                   )
                  -- OR
                  -- ret_rec.order_swtg_id IS NULL
                THEN
                    -- ���� �� ����� � ��� �� �������� ��������� �������, �� ������� ������ �� ���, ������� �����
                    -- ������� ���������������� (������� ��� ���������� 054-2007-� / 120-2006-�).
                    
                    l_Order_Step := l_Order_Step + 1;
                    
                    ret_rec.Order_Id := pk120_Bind_Operators_new.Get_XTTK_Op_Order_Id
                                                   (p_Start_Time   => ret_rec.Start_Time,
                                                    p_Sw_Name      => l_cur.Sw_Name,
                                                    p_Tg           => l_Tg, 
                                                    p_Ph_Number    => (CASE l_Side 
                                                                           WHEN 'A' THEN NVL(l_cur.order_ph_a, l_cur.cdr_abn_a)
                                                                           WHEN 'B' THEN NVL(l_cur.order_ph_b, l_cur.called_number)
                                                                       END),
                                                    p_RowNum        => l_Order_Step, -- ��� ���������� �������� ����� �� ������� (order_swtg_id) ���-��� ����������.
                                                    p_Order_Ph      => l_Order_Ph, 
                                                    p_UTC_Offset    => ret_rec.UTC_Offset,
                                                    p_Order_SwTg_Id => ret_rec.order_swtg_id
                                                   );            
                
                    IF ret_rec.Order_Id < 0 THEN
                       /*-- ���������� ������������ ��� ������, ��� �� � BDR-�� � CDR-�� �� ���� �����������
                        ret_rec.Order_Id := pin.pk00_const.c_TOO_MANY_ORDERS; */
                        -- ������ �� �������, ������������� ������ ������, �������
                        EXIT;
                    END IF;
                
                END IF;

                -- ���������� �� ������ ������� ���� � �������� ����            
                BEGIN        

                    ret_rec.BDR_Status := pk00_const.c_ACC_NOT_FOUND; -- -4
                            
                    -- ������ ��� ������ ��� � ������ � subpartition ����� 
                    IF l_Side = 'A' THEN
                        ret_rec.trf_type := pin.pk00_const.c_OP_RATE_PLAN_TYPE_DT;
                    ELSIF l_Side = 'B' THEN
                        ret_rec.trf_type := pin.pk00_const.c_OP_RATE_PLAN_TYPE_RT;
                    END IF;    
                    
                    SELECT op_sw_id, account_id, rateplan_id, order_swtg_id
                      INTO l_Op_Sw_Id, ret_rec.Account_Id, ret_rec.RatePlan_Id, ret_rec.order_swtg_id
                      FROM (SELECT os.op_sw_id, o.account_id, o.rateplan_id, os.order_swtg_id
                              FROM X07_ORDER_SWTG_T os,
                                   ORDER_T o
                             WHERE o.order_id = ret_rec.Order_Id 
                               AND o.order_id = os.order_id
                               AND (
                                    (ret_rec.order_swtg_id IS NOT NULL AND os.order_swtg_id = ret_rec.order_swtg_id)
                                     OR
                                    (ret_rec.order_swtg_id IS NULL AND 
                                     OS.SWITCH_ID = l_Sw_Id AND
                                     LOWER(os.trunkgroup) = LOWER(l_Tg) AND 
                                     (os.Trunkgroup_No = l_Order_Ph OR os.Trunkgroup_No IS NULL ) 
                                    )
                                   ) 
                            ORDER BY os.Trunkgroup_No NULLS LAST       
                            )
                      WHERE ROWNUM = 1;
                        
                EXCEPTION
                    WHEN no_data_found OR too_many_rows THEN
                         --if ret_rec.Order_Id != pin.pk00_const.c_TOO_MANY_ORDERS then                    
                           -- �������������� ������, ���������� ������
                            --Set_Counter;
                            --PIPE ROW (ret_rec); 
                            t_ret_col(l_Order_Step)(1) := ret_rec;
                            EXIT; -- ������� �� ����� ����������� 
                         --end if;   
                END;     
            
                IF ret_rec.Account_Id IS NOT NULL THEN
            
                   --- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                   -- ���������� ��� ��������� ���-�� ������ �������
                   -- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                   
                    --l_Trf_Step := 1;
                    l_Trf_Step := lt_Trf_Type.FIRST;
            
                    LOOP
                       -- ������� �� ����� ����������� ������� ����������     
                       -- ���� ������ �������� �������������� �� �� � ������� ����������������
                       -- ��� ������ ��� �������� ����������� (����. 3 ������� - ����., ������., ����. �� ��)
                        EXIT WHEN l_Trf_Step IS NULL OR
                                  (ret_rec.trf_type IN (pin.pk00_const.c_OP_RATE_PLAN_TYPE_RIP, pin.pk00_const.c_OP_RATE_PLAN_TYPE_DIP)
                                       AND 
                                   ret_rec.bdr_status IN (pin.pk00_const.c_VOL_PRICE_NOT_FOUND,
                                                          pin.pk00_const.c_RET_OK)
                                  );

                        l_TG_Tariff := FALSE; -- ��� �����. �� ���� �� ������ ����� ��� ������, �� �����
                                              -- ����. ������ �� �������� � ���� �������� ��� ��
                        l_Round_V_Id := NULL;
                
                        ret_rec.vol_type         := NULL;
                        ret_rec.op_rate_plan_id  := NULL;
                        ret_rec.subservice_id    := NULL;
                        ret_rec.service_id       := NULL;
                        ret_rec.parent_subsrv_id := NULL;
                        ret_rec.price            := NULL;  
                        ret_rec.price_id         := NULL;
                        ret_rec.Amount           := NULL; 
                    
                       -- ������� ��� ������, ������� ������ ��������� �� ������ ����
                        ret_rec.trf_type := lt_Trf_Type(l_Trf_Step);
                    
                      -- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
                      -- ����������� ����������
                      --- 
                      
                        BEGIN 
                           -- ���� �������� ���� ��� ������� �����      
                            ret_rec.op_rate_plan_id := lt_Rate_Plan(ret_rec.RatePlan_Id)(ret_rec.trf_type).op_rate_plan_id;
                            ret_rec.vol_type := lt_Rate_Plan(ret_rec.RatePlan_Id)(ret_rec.trf_type).tariff_vol_type;
                            l_Round_V_Id     := lt_Rate_Plan(ret_rec.RatePlan_Id)(ret_rec.trf_type).round_v_id;
                                                  
                        EXCEPTION
                            WHEN no_data_found THEN
                                ret_rec.BDR_Status := pin.pk00_const.c_TARIFF_NOT_FOUND;
                        END;                           
                      
                        --- 
                        -- ����������� ������ ��� ������� ����� ������
                        ----
                        
                        IF l_Side = 'A' AND ret_rec.trf_type = pin.pk00_const.c_OP_RATE_PLAN_TYPE_RIP -- 6
                            AND
                           ret_rec.op_rate_plan_id IS NOT NULL -- �� ���������  
                        THEN
                        
                           -- �������� ��� ��� �������� ������ ����������� �� ����� ��
                            ret_rec.BDR_Status :=
                                 Get_XTTK_R_RP_ID(p_Op_Rate_Plan_Id => ret_rec.op_rate_plan_id,
                                                  p_OP_Sw_Id        => l_Op_Sw_Id,
                                                  p_Phone           => ret_rec.Abn_B,
                                                  p_Date            => ret_rec.Bill_Date,
                                                  p_SubSrv_Id       => ret_rec.subservice_id  -- OUT
                                                 ); 
                
                        ELSIF l_Side = 'A' AND ret_rec.trf_type = pin.pk00_const.c_OP_RATE_PLAN_TYPE_DT
                               AND
                              ret_rec.op_rate_plan_id IS NOT NULL -- �� ���������
                        THEN
                            -- ���� ����� ����������
                            ret_rec.BDR_Status := 
                                Get_XTTK_D_RP_ID(p_Op_Rate_Plan_Id => ret_rec.op_rate_plan_id,
                                                 p_Switch_Id       => l_Sw_Id,
                                                 p_Phone           => ret_rec.Abn_B,
                                                 p_Date            => ret_rec.Bill_Date,
                                                 p_SubSrv_Id       => ret_rec.subservice_id  -- OUT,
                                                );                

                        ELSIF l_Side = 'A' AND ret_rec.trf_type = pin.pk00_const.c_OP_RATE_PLAN_TYPE_RI -- 4
                               AND
                              ret_rec.op_rate_plan_id IS NOT NULL -- �� ���������                        
                        THEN
                        
                            -- ���� ������ �������������
                            IF l_Loc_Call = 1                             
                            THEN                    
                                 -- ���������� �������
                                 ret_rec.BDR_Status :=
                                       Get_XTTK_R_RP_ID(p_Op_Rate_Plan_Id => ret_rec.op_rate_plan_id,
                                                        p_OP_Sw_Id    => l_Op_Sw_Id,
                                                        p_Phone       => ret_rec.Abn_A,
                                                        p_Date        => ret_rec.Bill_Date,
                                                        p_SubSrv_Id   => ret_rec.subservice_id  -- OUT
                                                       );       
                                                                                             
                            ELSE                   
                               
                                l_TG_Tariff := FALSE;                            
                            
                                BEGIN
                                    -- ��������� ����� � DEF ��� ���
                                    l_Prefix := Get_Def(ret_rec.Abn_B, l_cur.Sw_Name);
                                    
                                    IF l_Prefix IS NULL THEN -- �� DEF, �������
                                        RAISE no_data_found;
                                    END IF;
                                
                                    -- ����� DEF. ������ ������
                                     ret_rec.subservice_id := t_X07_Srv('ZINIT001'); -- ������ �������� �������������
                                     
                                     ret_rec.BDR_Status := pin.pk00_const.c_Ret_OK; -- ������ �������
                                    
                                EXCEPTION
                                    WHEN no_data_found THEN
                                    
                                       -- ����� � �� DEF. �.�. ������ ��/��
                                        BEGIN
                                        
                                            IF l_cur.tg_out = 'ROSTO' THEN -- �� �����������
                                                
                                                ret_rec.subservice_id := t_X07_Srv('IINIT003'); -- ������������� �� ���� ����������� 
                                                                       
                                                l_TG_Tariff := TRUE; -- �������, ��� ����� ������ ��� ��     
                                                 
                                            ELSIF l_cur.tg_out IN ('KTTKO','����40') THEN -- �� ���
                                                
                                                ret_rec.subservice_id := t_X07_Srv('IINIT002'); -- ������������� �� ���� �������������
                                            
                                                l_TG_Tariff := TRUE; -- �������, ��� ����� ������ ��� ��         
                                    
                                            ELSE
                                            
                                                ret_rec.subservice_id := t_X07_Srv('IINIT001'); -- ������������� �� ����
                                            
                                            END IF; 
                                   
                                            ret_rec.BDR_Status := pin.pk00_const.c_Ret_OK; -- ������ �������
                                                  
                                        EXCEPTION
                                            WHEN no_data_found THEN
                                                ret_rec.BDR_Status := pin.pk00_const.�_SRV_NOT_FOUND; -- �� ����� ������                              
                                        END;
           
                                END;     
                            
                            END IF;     

                        ELSIF l_Side = 'B' AND ret_rec.trf_type = pin.pk00_const.c_OP_RATE_PLAN_TYPE_DIP -- 5
                               AND
                              ret_rec.op_rate_plan_id IS NOT NULL -- �� ���������                        
                        THEN
                        
                            -- �������� ��� ��� ��������� ������ ������� �� ��
                            ret_rec.BDR_Status := 
                                Get_XTTK_D_RP_ID(p_Op_Rate_Plan_Id => ret_rec.op_rate_plan_id,
                                                 p_Switch_Id   => l_Sw_Id,
                                                 p_Phone       => ret_rec.Abn_B,
                                                 p_Date        => ret_rec.Bill_Date,
                                                 p_SubSrv_Id   => ret_rec.subservice_id  -- OUT
                                                );         
                                                    
                        --ELSIF l_Side = 'B' AND l_Trf_Step = 2 THEN
                               --ret_rec.trf_type := pin.pk00_const.c_OP_RATE_PLAN_TYPE_DI; -- 2                        
                        ELSIF l_Side = 'B' AND ret_rec.trf_type = pin.pk00_const.c_OP_RATE_PLAN_TYPE_DI -- 2
                               AND
                              ret_rec.op_rate_plan_id IS NOT NULL -- �� ���������                        
                        THEN

                            -- ���� ����� �������������
                            ret_rec.BDR_Status := 
                                  Get_XTTK_D_RP_ID(p_Op_Rate_Plan_Id => ret_rec.op_rate_plan_id,
                                                   p_Switch_Id       => l_Sw_Id,
                                                   p_Phone           => ret_rec.Abn_A,
                                                   p_Date            => ret_rec.Bill_Date,
                                                   p_SubSrv_Id       => ret_rec.subservice_id  -- OUT
                                                  ); 
                                                  
                            IF ret_rec.BDR_Status != 0 AND 
                               l_Loc_Call = 0 -- -- ������ �� �������. 
                            THEN
                                
                                l_TG_Tariff := FALSE;
                                
                                -- ��������� ����� � DEF ��� ���
                                l_Prefix := Get_Def(ret_rec.Abn_B, l_cur.Sw_Name);
                                    
                                IF l_Prefix IS NULL THEN -- �� DEF, ��/�� ����������

                                   -- ����� � �� DEF. �.�. ������ ��/��
                                    BEGIN
                                        IF l_TG = 'ROSTO' THEN -- �� �����������
                                                                                    
                                            ret_rec.subservice_id := t_X07_Srv('IINIT003'); -- ������������� �� ���� �����������
                                                      
                                            l_TG_Tariff := TRUE; -- �������, ��� ����� ������ ��� ��     
                                                 
                                        ELSIF l_TG IN ('KTTKO','KTTK40') THEN -- �� ���
                                                
                                            ret_rec.subservice_id := t_X07_Srv('IINIT002'); -- ������������� �� ���� �������������                                                
                                                
                                            l_TG_Tariff := TRUE; -- �������, ��� ����� ������ ��� ��         
                                    
                                        ELSE
                                            
                                            ret_rec.subservice_id := t_X07_Srv('IINIT001'); -- ������������� �� ����                                                                            
                                            
                                        END IF; 
                                   
                                        ret_rec.BDR_Status := pin.pk00_const.c_Ret_OK;
                                                  
                                    EXCEPTION
                                        WHEN no_data_found THEN
                                            ret_rec.BDR_Status := pin.pk00_const.�_SRV_NOT_FOUND; -- �� ����� ������                              
                                    END;

                                ELSE -- ������ �� DEF
                                   -- ++++ ����� ��������� 03.07.2015 �� ������������ � ������� �.�. ��� �� ������� ������ +++++
                                   --  ��� � ������ ������������� �� DEF ������ ������ ���� ���� ��������� � �������� ������� ������� 
                                    -- ����� DEF. 
                                     ret_rec.subservice_id := t_X07_Srv('ZINIT001'); -- ������ �������� �������������  
                                                                          
                                     ret_rec.BDR_Status := pin.pk00_const.c_Ret_OK; -- ������ �������
                                   -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ 
 
                                END IF;
                                
                            END IF;     
                            
                        --ELSIF l_Side = 'B' AND l_Trf_Step = 3 THEN
                            --ret_rec.trf_type := pin.pk00_const.c_OP_RATE_PLAN_TYPE_RT; -- 3                        
                        ELSIF l_Side = 'B' AND ret_rec.trf_type = pin.pk00_const.c_OP_RATE_PLAN_TYPE_RT -- 5
                               AND
                              ret_rec.op_rate_plan_id IS NOT NULL -- �� ���������                        
                        THEN
                              
                            -- ���� ������ ����������
                            ret_rec.BDR_Status :=
                                Get_XTTK_R_RP_ID(p_Op_Rate_Plan_Id =>ret_rec.op_rate_plan_id,
                                                 p_OP_Sw_Id    => l_Op_Sw_Id,
                                                 p_Phone       => ret_rec.Abn_B,
                                                 p_Date        => ret_rec.Bill_Date,
                                                 p_SubSrv_Id   => ret_rec.subservice_id  -- OUT
                                                );                  

                        END IF;

                      -- ���� ������ ���� �� ������. ���� ������ ��������� �� �����. �� � ����
                      -- ������� �� �����, �� ����� ������ �� ������������ ������
                        LOOP 
                             
                           -- �������� servi�e_id � subservice_id �� �� 
                            IF ret_rec.BDR_Status = pin.pk00_const.c_RET_OK THEN
                                
                                BEGIN
                                    -- �������� ������ � ������ billing-�
                                    ret_rec.service_id := t_X07_SubSrv(ret_rec.subservice_id).service_id;  
                                    ret_rec.parent_subsrv_id := t_X07_SubSrv(ret_rec.subservice_id).parent_subsrv_id;
                                     
                                EXCEPTION
                                    WHEN no_data_found THEN
                                        NULL;                          
                                END; 
                                   
                            END IF;

                            IF ret_rec.BDR_Status = pin.pk00_const.c_RET_OK THEN
                            
                                -- �������� �������������� �����   
                                ret_rec.bill_minutes := PK1110_OPR_TARIFFING_NEW.Get_Bill_Duration(p_Seconds         => ret_rec.Duration,
                                                                                                   p_Op_Rate_Plan_Id => ret_rec.Op_Rate_Plan_Id,
                                                                                                   p_Round_V_Id      => l_Round_V_Id);
                             
                                IF ret_rec.vol_type = pin.pk00_const.c_TARIF_VOL_TYPE_NO THEN
                                   -- ����� �� ��������, �������� ���� 
                                    ret_rec.BDR_Status := 
                                          Get_XTTK_RP_Price(p_Op_RP_Id  => ret_rec.op_rate_plan_id,
                                                            p_SubSrv_Id => ret_rec.subservice_id, 
                                                            p_Date      => ret_rec.Bill_Date,
                                                            p_Price     => ret_rec.price,    -- OUT
                                                            p_Price_Id  => ret_rec.price_id  -- OUT (rec_id)
                                                           );
                                                               
                                   -- �������� ���� ����������                         
                                    ret_rec.Amount  := ret_rec.Price * ret_rec.Bill_Minutes;

                                ELSIF ret_rec.vol_type = pin.pk00_const.c_TARIF_VOL_TYPE_VOL THEN
                                   -- ������ ��������������� ������. ������ ����������� ����� ����� ������
                                   -- �����, ��������������� ������ �������
                                    ret_rec.bdr_status := pin.pk00_const.c_VOL_PRICE_NOT_FOUND; -- -8  
                                    ret_rec.Price  := 0;
                                    ret_rec.Amount := 0;

                                END IF;
                                
                            END IF;    

                            EXIT WHEN l_TG_Tariff = FALSE OR 
                                      ret_rec.BDR_Status IN (pin.pk00_const.c_RET_OK,pin.pk00_const.c_VOL_PRICE_NOT_FOUND);

                            IF l_TG_Tariff = TRUE THEN
                               -- ���� ������ ���� ���������� �� ��, �� ���������� �� ������������������,
                               -- �� �������� ��� ����. ����������������� �� ������������ ������ 
                                l_TG_Tariff := FALSE;
                                
                                BEGIN
                                
                                    ret_rec.subservice_id := t_X07_Srv('IINIT001'); -- ������������� �� ����
                                    ret_rec.service_id    := t_X07_SubSrv(ret_rec.subservice_id).service_id;  
                                    ret_rec.parent_subsrv_id := t_X07_SubSrv(ret_rec.subservice_id).parent_subsrv_id;
                                
                                   -- �������� ���� ��� ���� � ����������� ����, ����� ����� �������� ��� ����� ������
                                    ret_rec.BDR_Status := pin.pk00_const.c_RET_OK;   
                                                               
                                EXCEPTION
                                    WHEN no_data_found THEN
                                       ret_rec.BDR_Status := pin.pk00_const.�_SRV_NOT_FOUND; -- �� ����� ������
                                END;    

                            END IF;
                             
                        END LOOP;
                      
                      -- ���� ������ �������� �������������� �� �� (���. ��� ����.) � ������� ���������������� 
                      -- ��� ������� ����������� ��� ��������������, �� ����� �����/������ � ������� ���
                      -- �� ������� ����������������, �� ���������� � ��� �� ������� � ���������   
                        /*IF (l_Trf_Step = 1 AND ret_rec.bdr_status IN (pin.pk00_const.c_VOL_PRICE_NOT_FOUND,
                                                                      pin.pk00_const.c_RET_OK)
                           )                                           
                            OR            
                            l_Trf_Step > 1*/
                        IF (ret_rec.trf_type IN (pin.pk00_const.c_OP_RATE_PLAN_TYPE_RIP, pin.pk00_const.c_OP_RATE_PLAN_TYPE_DIP)
                             AND                                    
                            ret_rec.bdr_status IN (pin.pk00_const.c_VOL_PRICE_NOT_FOUND, pin.pk00_const.c_RET_OK)
                           )
                           OR
                           ret_rec.trf_type NOT IN (pin.pk00_const.c_OP_RATE_PLAN_TYPE_RIP, pin.pk00_const.c_OP_RATE_PLAN_TYPE_DIP)  
                        THEN                                             
         
                            Set_Counter;
                            
                            IF ret_rec.trf_type IN (pin.pk00_const.c_OP_RATE_PLAN_TYPE_DI,
                                                    pin.pk00_const.c_OP_RATE_PLAN_TYPE_DIP,
                                                    pin.pk00_const.c_OP_RATE_PLAN_TYPE_DT) 
                               AND
                               ret_rec.Order_Id > 0                     
                            THEN
                               -- ���� �����, �� ���������� bill_id
                                BEGIN
                                    -- ���� � ��� �����������
                                    ret_rec.bill_id := t_Bill(ret_rec.Order_Id);
                                EXCEPTION
                                    WHEN no_data_found THEN    
                                        ret_rec.bill_id := pk114_items.Get_Bill_Id(p_Order_Id  => ret_rec.Order_Id,
                                                                                   p_Period_Id => p_Period_Id,
                                                                                   p_Job_Id    => p_Task_Id
                                                                                  );
                                        t_Bill(ret_rec.Order_Id) := ret_rec.bill_id;                                           
                                END;      
                            END IF;
                            
                           -- ��� ������� ������������������� ������� ���������� order_body_id
                           -- � ������ �������� �� �-������, ������� �� ������ �������� � ����  
                            IF ret_rec.bdr_status IN (pin.pk00_const.c_VOL_PRICE_NOT_FOUND,
                                                      pin.pk00_const.c_RET_OK) 
                            THEN                                              
                               -- ���������� order_body_id
                                BEGIN
                                
                                   -- �������� ������� ������!!!!
                                    SELECT order_body_id
                                      INTO ret_rec.order_body_id
                                      FROM (
                                            SELECT b.order_body_id
                                              FROM order_body_t b
                                             WHERE b.order_id = ret_rec.Order_Id
                                               AND b.charge_type = pk00_const.c_CHARGE_TYPE_USG
                                               AND b.subservice_id = ret_rec.subservice_id
                                               AND ret_rec.bill_date BETWEEN b.date_from AND b.date_to
                                            ORDER BY modify_date DESC
                                           )
                                     WHERE ROWNUM = 1;           
                                      
                                EXCEPTION
                                    WHEN no_data_found THEN
                                            
                                       -- ���� ������ � order_body_t, �� ��� ��� �������� �������. 
                                       -- ������ ������ ������ order_body_id
                                        ret_rec.bdr_status := t_BDR_Status('OB_ID_ERR'); -- -25
   
                                END;                                                   

                                BEGIN
                                   -- ��������, �.�. � ��������� ������� ����. � �����. � ������� �� ������ ���������� � ����
                                    SELECT t_BDR_Status('NOT_BILL') -- -28
                                      INTO ret_rec.bdr_status
                                      FROM x07_bill_exclude e 
                                     WHERE e.order_id = ret_rec.Order_Id
                                       AND e.phone_num = ret_rec.Abn_A
                                       AND ret_rec.bill_date BETWEEN e.date_from AND NVL(e.date_to, gc_MaxDate)
                                       AND ROWNUM = 1; -- ����� ��������� ������ ���������
                                        
                                EXCEPTION
                                    WHEN no_data_found THEN
                                        NULL; -- � ����������� ������ �� ������� 
                                END; 
                                                                          
                            END IF;        
                          
                           -- ���������� ������������������� ������
                            t_ret_col(l_Order_Step)(l_Trf_Step) := ret_rec; 
                            
                            IF ret_rec.bdr_status IN (pin.pk00_const.c_VOL_PRICE_NOT_FOUND,
                                                      pin.pk00_const.c_RET_OK,
                                                      t_BDR_Status('NOT_BILL'), -- ����� ����������� �� �����������
                                                      t_BDR_Status('OB_ID_ERR') -- �� ������ order_body_t
                                                     ) 
                            THEN
                                -- ������� ��� ����������� ���� ��������. ������� ���������� ������, ���� ����� ����
                                l_Order_Step := t_ret_col.PRIOR(l_Order_Step);
                                
                                WHILE l_Order_Step IS NOT NULL 
                                LOOP
                                  t_ret_col.DELETE(l_Order_Step);  
                                  l_Order_Step := t_ret_col.PRIOR(l_Order_Step);
                                END LOOP;                                
                                 
                                -- ������� ��� ��� ���������� �������� ���������� �������� ������� �� ���������,
                                -- �.�. � ������� ���� �������� �����������
                                l_Order_Step := -1; 
                            END IF;
                            
                        END IF;    
                      
                       -- ������� �� ����� ����������� ������� ����������     
                       -- ���� ������ �������� �������������� �� �� � ������� ����������������
                       -- ��� ������ ��� �������� ���������� (����. 3 ������� - ����., ������., ����. �� ��)
                        /*EXIT WHEN l_Trf_Step = 3 OR
                                  (l_Trf_Step = 1 AND ret_rec.bdr_status IN (pin.pk00_const.c_VOL_PRICE_NOT_FOUND,
                                                                             pin.pk00_const.c_RET_OK)
                                  );
                        
                        l_Trf_Step := l_Trf_Step + 1;  */
                        
                        l_Trf_Step := lt_Trf_Type.NEXT(l_Trf_Step);
                        
                    END LOOP;          
                ---      
                --- ������������ ��� ��������� ���-�� ������� 
                --- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

                END IF;      

                EXIT WHEN (
                           l_Side = 'A' AND l_cur.order_id_a != pin.pk00_const.c_TOO_MANY_ORDERS 
                            OR
                           l_Side = 'B' AND l_cur.order_id_b != pin.pk00_const.c_TOO_MANY_ORDERS 
                          ) -- ������� ������  
                          OR 
                          ( 
                           (l_Side = 'A' AND l_cur.order_id_a = pin.pk00_const.c_TOO_MANY_ORDERS 
                            OR
                            l_Side = 'B' AND l_cur.order_id_b = pin.pk00_const.c_TOO_MANY_ORDERS
                           ) 
                            AND 
                           (
                            ret_rec.Order_Id < 0 -- ������� ��������, �� ����� �� ������ ��� ��� ��� ���������
                             OR
                            l_Order_Step = -1 -- ������� ��������, �� ������ �����, ������������������� �������
                           ) 
                          ); 
                         
            END LOOP; -- ������� ���� ������� ��� ���������� ��������

            -- ���������� ������������������� ������
            l_Order_Step := t_ret_col.FIRST();
            WHILE l_Order_Step IS NOT NULL
            LOOP
            
              l_Trf_Step := t_ret_col(l_Order_Step).FIRST();

              WHILE l_Trf_Step IS NOT NULL 
              LOOP
                PIPE ROW (t_ret_col(l_Order_Step)(l_Trf_Step));  
                l_Trf_Step := t_ret_col(l_Order_Step).NEXT(l_Trf_Step);
              END LOOP;        
              
              l_Order_Step := t_ret_col.NEXT(l_Order_Step);
              
            END LOOP;  
             
        END LOOP; -- ������� ������� �, ������� �

        FETCH pr_Call INTO l_cur;
         
    END LOOP; -- ������� ������� � ����������    

END Trf_Oper_Table;

BEGIN
    -- ������������� ������� � �������� BDR-��
    FOR l_cur IN (SELECT d.KEY, d.external_id
                    FROM PIN.DICTIONARY_T d
                   WHERE LEVEL = 2
                 CONNECT BY PRIOR d.key_id = d.parent_id
                   START WITH d.KEY = 'TRF_STAT'
                 )
    LOOP
        t_BDR_Status(l_cur.KEY) := l_cur.external_id; 
    END LOOP;        

    -- ������������� ������� � ��������
    FOR l_cur IN (SELECT srv_id, srv_key
                    FROM x07_srv_dct
                 )
    LOOP
        t_X07_Srv(l_cur.srv_key) := l_cur.srv_id;     
    END LOOP;                    

   -- ������������� �� ������� ����� ����� X �� ��������/�������� ��������
    FOR l_cur IN (SELECT s.srv_id, s.subservice_id, ss.service_id
                    FROM x07_srv_dct s,
                         service_subservice_t ss
                   WHERE s.subservice_id = ss.subservice_id(+)
                 )    
    LOOP
        t_X07_SubSrv(l_cur.srv_id).service_id := l_cur.service_id;
        t_X07_SubSrv(l_cur.srv_id).parent_subsrv_id := l_cur.subservice_id;  
    END LOOP;                                   

    -- ������������� ������� BDR - ����
    FOR l_cur IN (SELECT b.bdr_type_id, b.bdr_code, n.network_code
                    FROM bdr_types_t b,
                         network_t n
                   WHERE b.network_id = n.network_id)
    LOOP
        t_BDR_Network(l_cur.bdr_type_id).bdr_code     := l_cur.bdr_code;
        t_BDR_Network(l_cur.bdr_type_id).network_code := l_cur.network_code;     
    END LOOP;                      

    -- ������������� ������� � �������������
    FOR l_cur IN (SELECT switch_code, switch_id
                    FROM switch_t)
    LOOP
        t_Switch(l_cur.switch_code) := l_cur.switch_id;     
    END LOOP;                  

    -- ������������� ������� � �������������
    -- ��������� �������� ABC
    FOR l_cur IN (SELECT a.prefix, z.z_name
                    FROM tariff_ph.d02_zone_abc a,
                         tariff_ph.d01_zone z
                   WHERE a.abc_h_id = z.abc_h_id)
    LOOP
    
        t_Prefix(l_cur.prefix) := l_cur.z_name;
    
    END LOOP;               
                   
    -- ��������� �������� DEF
    FOR l_cur IN (SELECT prefix, z_name
                    FROM (
                           SELECT d.prefix, z.z_name,
                                 row_number() OVER (PARTITION BY d.prefix ORDER BY z_type ASC) rn -- ����� ������� �������  
                            FROM tariff_ph.d03_zone_def d,
                                 tariff_ph.d01_zone z
                           WHERE d.def_h_id = z.def_h_id
                             AND NOT EXISTS (SELECT 1 -- �� ������ ������ ��������� ��������� ������ 
                                               FROM tariff_ph.d02_zone_abc a
                                              WHERE a.prefix = d.prefix)  
                         )
                   WHERE rn = 1         
                 )                     
    LOOP
    
        t_Prefix(l_cur.prefix) := l_cur.z_name;
    
    END LOOP;                   
                     
    -- �������� ����. ����� �������� �����������
    SELECT MAX(max_length)
      INTO gl_Max_Prefix
      FROM (SELECT MAX(LENGTH(prefix)) max_length
              FROM tariff_ph.d02_zone_abc
            UNION  
            SELECT MAX(LENGTH(prefix)) max_length
              FROM tariff_ph.d03_zone_def
           );   

END PK1110_OPR_TARIFFING_NEW;
/
