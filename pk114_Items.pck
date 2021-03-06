CREATE OR REPLACE PACKAGE pk114_Items
IS

    gc_PkgName CONSTANT varchar2(16) := 'pk114_Items';

    gc_SRC_AUDIT  CONSTANT NUMBER := 0; -- ������ ��� ����������� ����� �� ������ ������
    gc_ALL_ERRORS CONSTANT NUMBER := -1; -- ������������� ��� ������    
    gc_NEW_CDR    CONSTANT NUMBER := -2; -- ��������� ������ ��� ����� CDR-�, �.�. ������� ��� �� �������� � �������

    -- ������� ��� �������� ������������ ��������� ��������� ������� (p_Rep_Period) ���
    -- ��������� ���� ������������/��������������� (p_Source) ��� �������, ���������� ��  
    -- ����� � p_Date_From �� p_Date_To 
    -- ���������� 0 - ������� ������ ����������� ��� ������ ������
    --            1 - ������� ������ ���������, ������ ������ 
    FUNCTION Check_Rep_Period(p_Date_From  IN date,
                              p_Date_To    IN date,
                              p_Rep_Period IN date
                             ) RETURN number;

    FUNCTION Get_External_Id(p_BDR_Type IN varchar2
                            ) RETURN number; 

    -- ��� ������ - MMTS, ZONE, SPB � �.�. (�� dictionaty bdr_source)
    -- p_In_Out -- D - bdr-� ����������� � ������, 
    --             R - bdr-� ����������� � �������
    --             NULL - �����
    FUNCTION Get_List_BDR_Types(p_Data_Type IN varchar2,
                                p_Side      IN varchar2 DEFAULT NULL,
                                p_In_Out    IN varchar2 DEFAULT NULL  -- D - �����, � - ������
                               ) RETURN varchar2;


    FUNCTION Get_Period_Date(p_Day       date, -- ����, ��� �������� ���� ������� �������� ������
                             p_Calc_Date date DEFAULT SYSDATE -- ������, �� ������� ���� ��� ���������� ��� ����. ������
                            ) RETURN date;


    PROCEDURE Set_Bill_Id(p_Data_Table    IN varchar2,
                          p_Rep_Period_Id IN number,
                          p_Task_Id       IN number
                         );

    /*  ������� ��� ���������� bill_id ��� �������� ������ � �������
       ������� ���������:
           p_Order_Id  - ������������� ������
           p_Period_Id - �������� ������
           p_Job_Id    - id ������� �� ������������/��������������. (����� ����� ���� ������������� ����� bill_id)
       ���������� 0 � ������ ������ ��� ��������������� ��� ������    
    */
    FUNCTION Get_Bill_Id(p_Order_Id  IN number,
                         p_Period_Id IN number,
                         p_Job_Id    IN number DEFAULT NULL
                        ) RETURN number PARALLEL_ENABLE; 

    -- ������� ����� ������ (item_t) � �� ����������� (detail_mmts_t)
    -- �������� BDR-��, �� ������� ����� ���� ��������: MMTS, SAMARA, SPB
    PROCEDURE Load_BDR_to_Item(p_Period     IN date,
                               p_Data_Type  IN varchar2,
                               p_Account_Id IN number   DEFAULT NULL);                            

    -- ������ ��������� ����������
    PROCEDURE Load_Op_MinPay(p_Data_Type  IN varchar2,
                             p_Rep_Period IN date,
                             p_Call_Month IN date DEFAULT NULL
                            ); 

END pk114_Items;
/
CREATE OR REPLACE PACKAGE BODY pk114_Items
IS

    gc_DATE_END CONSTANT date := TO_DATE('01.01.2050','dd.mm.yyyy'); 


FUNCTION Get_External_Id(p_BDR_Type IN varchar2
                        ) RETURN number 
IS

    l_External_Id number;

BEGIN

    SELECT b.bdr_type_id
      INTO l_External_Id
      FROM bdr_types_t b
     WHERE b.bdr_code = p_BDR_Type;

    RETURN l_External_Id;

END Get_External_Id;

-- ��� ������ - MMTS, ZONE, SPB � �.�. (�� dictionaty bdr_source)
-- p_In_Out -- D - bdr-� ����������� � ������, 
--             R - bdr-� ����������� � �������
--             NULL - �����
FUNCTION Get_List_BDR_Types(p_Data_Type IN varchar2,
                            p_Side      IN varchar2 DEFAULT NULL,
                            p_In_Out    IN varchar2 DEFAULT NULL  -- D - �����, � - ������
                           ) RETURN varchar2
IS

    l_BDR_Type varchar2(16);
    l_BDR_Tbl  varchar2(32);
    l_Agent    number;
    l_Items    number;
    l_Oper     number;
BEGIN

    l_BDR_Type := PIN.Get_BDR_Type_Full(p_Data_Type => p_Data_Type,
                                        p_BDR_Table => l_BDR_Tbl,
                                        p_Agent     => l_Agent,
                                        p_Items     => l_Items,
                                        p_Oper      => l_Oper 
                                       );

    CASE 
        WHEN l_Oper = 1
         AND p_Side = 'A' 
         AND p_In_Out IS NULL
        THEN
        
            l_BDR_Type := TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_DT) || ',' ||
                          TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_RI) || ',' ||
                          TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_RIP);
                                      
        WHEN l_Oper = 1
         AND p_Side = 'A' 
         AND p_In_Out = 'D'
        THEN                          
        
            l_BDR_Type := TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_DT);        
        
        WHEN l_Oper = 1
         AND p_Side = 'A' 
         AND p_In_Out = 'R'
        THEN                          
        
            l_BDR_Type := TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_RI) || ',' ||
                          TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_RIP);        
                          
        WHEN l_Oper = 1
         AND p_Side = 'B' 
         AND p_In_Out IS NULL
        THEN
         
            l_BDR_Type := TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_DI) || ',' ||
                          TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_RT) || ',' ||
                          TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_DIP);         
                                                  
       WHEN l_Oper = 1
         AND p_Side = 'B' 
         AND p_In_Out = 'D'
        THEN
         
            l_BDR_Type := TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_DI) || ',' ||
                          TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_DIP);                          
                          
       WHEN l_Oper = 1
         AND p_Side = 'B' 
         AND p_In_Out = 'R'
        THEN
         
            l_BDR_Type := TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_RT);       
            
        WHEN l_Oper = 1
         AND p_Side IS NULL 
         AND p_In_Out = 'D'
        THEN
         
            l_BDR_Type := TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_DI) || ',' ||
                          TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_DT) || ',' ||
                          TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_DIP);                              
                          
        WHEN l_Oper = 1
         AND p_Side IS NULL 
         AND p_In_Out = 'R'
        THEN
         
            l_BDR_Type := TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_RI) || ',' ||
                          TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_RT) || ',' ||
                          TO_CHAR(pin.pk00_const.c_OP_RATE_PLAN_TYPE_RIP);  
                          
        ELSE
           -- ��������� �������� l_BDR_Type ��� ���������
            NULL;                                                       
    
    END CASE;

    RETURN l_BDR_Type;

END Get_List_BDR_Types;


FUNCTION Get_Period_Date(p_Day       date, -- ����, ��� �������� ���� ������� �������� ������
                         p_Calc_Date date DEFAULT SYSDATE -- ������, �� ������� ���� ��� ���������� ��� ����. ������
                        ) RETURN date
IS

    l_Rep_Period number;

BEGIN

   -- �������� ������� �������� ������
    SELECT period_id  
      INTO l_Rep_Period
      FROM PERIOD_T r
     WHERE p_Day BETWEEN r.period_from AND r.period_to
       AND (
            r.close_rep_period IS NULL
             OR
            r.close_rep_period > p_Calc_Date 
           );
                
    -- ��������� ���� �������� � ���������� ������. ���������� ��������� ����.              
     RETURN p_Day;
                 
EXCEPTION
    WHEN no_data_found THEN
                
       -- ��������� ���� �� �������� � ���������� ������. 
       -- ���������� ������ ���� ������ ���������� ����������� �������.
        SELECT period_id  
          INTO l_Rep_Period
          FROM (
                SELECT period_id
                  FROM PERIOD_T r
                 WHERE r.period_to > p_Day 
                   AND (
                        r.close_rep_period IS NULL
                         OR
                        r.close_rep_period > p_Calc_Date 
                       )
                  ORDER BY period_from ASC
                )
          WHERE ROWNUM = 1;
                           
         RETURN TO_DATE(l_Rep_Period,'YYYYMM');                                   
    
END Get_Period_Date;

PROCEDURE Set_Bill_Id(p_Data_Table    IN varchar2,
                      p_Rep_Period_Id IN number,
                      p_Task_Id       IN number
                     )
IS
BEGIN

    EXECUTE IMMEDIATE
        'MERGE INTO ' || p_Data_Table || ' b ' || CHR(10) ||
        'USING (SELECT new_order_id, ' || CHR(10) ||
        '              pk114_Items.Get_Bill_Id(b.new_order_id, :l_Rep_Period_Id, :l_Task_Id) bill_id ' || CHR(10) ||
        '         FROM ' || p_Data_Table || ' b ' || CHR(10) ||
        '        GROUP BY new_order_id ' || CHR(10) ||
        '      ) t ' || CHR(10) ||
        ' ON (b.new_order_id = t.new_order_id) ' || CHR(10) ||
        'WHEN MATCHED THEN UPDATE ' || CHR(10) ||
        ' SET b.bill_id = t.bill_id'
    USING p_Rep_period_Id, p_Task_Id;    

END Set_Bill_Id; 

/*  ������� ��� ���������� bill_id ��� �������� ������ � �������
   ������� ���������:
       p_Order_Id  - ������������� ������
       p_Period_Id - �������� ������
       p_Job_Id    - id ������� �� ������������/��������������. (����� ����� ���� ������������� ����� bill_id)
   ���������� bill_id � ������ ������ ��� ��������������� ��� ������ (�������� < 0)    
*/
FUNCTION Get_Bill_Id(p_Order_Id  IN number,
                     p_Period_Id IN number,
                     p_Job_Id    IN number DEFAULT NULL
                    ) RETURN number PARALLEL_ENABLE 
IS

    l_Bill_Status PIN.BILL_T.BILL_STATUS%TYPE;
    l_Bill_Id     PIN.BILL_T.BILL_ID%TYPE;
BEGIN
        
    IF NVL(p_Job_Id, -1) > 0 THEN
    
        -- �������� �������� bill_id �� �������
        EXECUTE IMMEDIATE
            'SELECT b.bill_id, b.bill_status ' || CHR(10) ||
            '  FROM Q01_RETRF_JOB_DETAIL q, ' || CHR(10) ||
            '       (SELECT b.bill_id, b.bill_status ' || CHR(10) ||
            '          FROM BILL_T b ' || CHR(10) ||
            '         WHERE b.rep_period_id = :p_Rep_Period_Id ' || CHR(10) ||
            '           AND b.bill_type IN (' || pk00_const.c_BTYPE_FOR_BDR || ')' || CHR(10) ||
            '       ) b ' || CHR(10) ||     
            ' WHERE q.bill_id IS NOT NULL ' || CHR(10) ||
            '   AND q.task_id      = :p_Job_Id ' || CHR(10) ||
            '   AND q.order_id_new = :p_Order_Id ' || CHR(10) ||
            '   AND b.bill_id(+) = q.bill_id'
        INTO l_Bill_Id, l_Bill_Status     
        USING p_Period_Id, 
              p_Job_Id, p_Order_Id;
           
        IF l_Bill_Id IS NULL THEN
            -- ���� �����, �� ��� ��� � bill_id ��� �� �� � ������� �������
            RETURN pk00_const.c_BILL_NOT_CORRECT;
            
        ELSIF l_Bill_Id IS NOT NULL AND l_Bill_Status NOT IN (pk00_const.c_BILL_STATE_OPEN) THEN
            -- ����� �������� ����
            RETURN pk00_const.c_BILL_IS_CLOSED;
                
        ELSE
        
            RETURN l_Bill_Id;
            
        END IF;        
                   
    ELSE
        RAISE no_data_found; -- ����� �����, ������������� ��-���������        
    END IF;
             
EXCEPTION
    WHEN no_data_found THEN
       -- ��� ��������� job_id � order_id ���� �� �����.     
        BEGIN
            -- ���� bill_id ��-���������
            SELECT bill_id, b.bill_status
              INTO l_Bill_Id, l_Bill_Status 
              FROM bill_t b,
                   order_t o
             WHERE b.bill_type = PK00_CONST.c_BILL_TYPE_REC
             --  AND b.bill_status IN (PK00_CONST.c_BILL_STATE_OPEN)
               AND b.rep_period_id = p_Period_Id
               AND o.order_id      = p_Order_Id
               AND b.account_id    = o.account_id;                
               
            IF l_Bill_Status NOT IN (PK00_CONST.c_BILL_STATE_OPEN) THEN
            
                l_Bill_Id := pk00_const.c_BILL_IS_CLOSED;
            
            END IF;
                
            RETURN l_Bill_Id;   
               
        EXCEPTION  -- ���������� ������
            WHEN no_data_found THEN
                RETURN pk00_const.c_NO_BILL_FOUND; 

            WHEN too_many_rows THEN
                RETURN pk00_const.c_TOO_MANY_BILLS; 
        END;
                
END Get_Bill_Id;        

/* ������� ��������� ������ ��� ��� ��������� ������ �� �������� ����
   ����������: 0 - ��� ������� � �������� ��������� ��� �������
               1 - ���� ���� �� ���� �������� ������ � �������� ��������� ���
*/
FUNCTION Check_Cls_Period(p_Date_From IN DATE, -- ���� ��������
                          p_Date_To   IN date,
                          p_Calc_Date IN date DEFAULT NULL -- ���� �������
                         ) RETURN INTEGER
IS

    l_Result INTEGER;
    
BEGIN
       
    SELECT 1 INTO l_Result
      FROM PERIOD_T r
     WHERE p_Date_From <= r.period_to
       AND p_Date_To   >= r.period_from 
       AND r.close_rep_period <= p_Calc_Date
       AND ROWNUM = 1;

    RETURN 1;   
    
EXCEPTION       
    WHEN NO_DATA_FOUND THEN
        RETURN 0;
END Check_Cls_Period;

-- ������� ��� �������� ������������ ��������� ��������� ������� (p_Rep_Period) ���
-- ��������� ���� ������������/��������������� (p_Source) ��� �������, ���������� ��  
-- ����� � p_Date_From �� p_Date_To 
-- ���������� 0 - ������� ������ ����������� ��� ������ ������
--            1 - ������� ������ ���������, ������ ������ 
FUNCTION Check_Rep_Period(p_Date_From  IN date,
                          p_Date_To    IN date,
                          p_Rep_Period IN date
                         ) RETURN number
IS

    v_prcName CONSTANT varchar2(16) := 'Check_Rep_Period';

    l_Result  number;

BEGIN

   --
   -- �������� ������������ �������, ���� ���� �������� ������������������� ������.
    IF TRUNC(p_Rep_Period,'mm') < TRUNC(p_Date_To,'mm') THEN
    
        Pk01_Syslog.Write_Msg(p_Msg   => '������ ������� �������. ' ||
                                         '������ ����� �� ����� ���� ������ ������ ���� ��������� �������. �������.', 
                              p_Src   => gc_PkgName || '.' || v_prcName,
                              p_Level => Pk01_Syslog.L_err);
        l_Result := 0;                          
    
    ELSE
    
        BEGIN
           -- �������� ���� �� �������� ������ � �������� ���������
            SELECT 1 INTO l_Result
              FROM PERIOD_T r
             WHERE NVL(p_Rep_Period, p_Date_From) <= r.period_to
               AND NVL(p_Rep_Period, p_Date_To)   >= r.period_from 
               AND r.close_rep_period <= SYSDATE
               AND ROWNUM = 1;

            Pk01_Syslog.Write_Msg(p_Msg   => '�������� �������� ������ ������.',
                                  p_src   => gc_PkgName || '.' || v_prcName,
                                  p_Level => Pk01_Syslog.L_err);             
                   
            l_Result := 0;            
            
        EXCEPTION       
            WHEN NO_DATA_FOUND THEN
                l_Result := 1;
        END;

         
    END IF;
    
    RETURN l_Result;

END Check_Rep_Period;  


-- �������� ����� ������ (item_t) � �� ����������� (detail_mmts_t)
-- �������� BDR-��, �� ������� ����� ���� ��������: MMTS, SAMARA, SPB
PROCEDURE Load_BDR_to_Item(p_Period     IN date,
                           p_Data_Type  IN varchar2,
                           p_Account_Id IN number   DEFAULT NULL)
IS

    c_prcName CONSTANT varchar2(20) := 'Load_BDR_to_Item';

    l_Cursor  sys_refcursor;

    l_Date_From     date;
    l_Date_To       date;
    l_Prev_From     date;
    l_Prev_To       date;
    l_BDR_Table     varchar2(32);
    l_BDR_Type_Id   number;
    l_Trf_Type      varchar2(16); -- ��� BDR-�� � ������� ���� ������� (���������)
    l_Rep_Period_Id number;
    l_Calc_Date     date;
    l_External_Id   number;
    l_Order_Id      number;
    l_Subservice_Id number;
    l_RatePlan_Id   number;
    
    l_Insert        number;
    l_Update        number;
    l_Delete        number; 
    l_Count         PLS_INTEGER;
    l_Moved         number;
    
    l_Ord_DFrom     date;
    l_Ord_DTo       date;
    
    l_RowId rowid;
    
    l_OB_Not_Found number;

BEGIN

    Pk01_Syslog.write_Msg(p_Msg   => 'Load Item. ' || 
                                     'Period ' || TO_CHAR(p_period, 'dd.mm.yyyy') ||
                                     ', Type: ' || NVL(p_Data_Type,'ALL'), 
                          p_Src   => gc_PkgName || '.' || c_prcName);

    l_Calc_Date := SYSDATE; -- ���� ������ ��������� item-��

    l_Rep_Period_Id := pk00_const.Get_Period_Id(p_Period);

    l_Date_From := TRUNC(p_Period,'mm');
    l_Date_To   := LAST_DAY(TRUNC(p_Period,'mm')) + INTERVAL '00 23:59:59' DAY TO SECOND;

   -- �������� ��� �������
    l_BDR_Type_Id := PIN.Get_BDR_Type(p_Data_Type => UPPER(p_Data_Type),
                                   p_BDR_Table => l_BDR_Table, -- out
                                   p_Agent     => l_Count,      -- out (����� �� ������������)
                                   p_Items     => l_Insert
                                  );

    IF l_BDR_Table IS NULL THEN
        
        Pk01_Syslog.write_Msg(p_Msg   => '�������� BDR-�� �� ������.', 
                              p_Src   => gc_PkgName || '.' || c_prcName,
                              p_Level => Pk01_Syslog.L_err);
                              
        RETURN;                
    
    ELSIF l_Insert = 0 THEN -- �������� ��� ������ � item-� �� ������������           
    
        Pk01_Syslog.write_Msg(p_Msg   => p_Data_Type || ' �� �������� ����������� � item_t.', 
                              p_Src   => gc_PkgName || '.' || c_prcName,
                              p_Level => Pk01_Syslog.L_err);
                              
        RETURN;                    
    
    END IF;

    -- ���������� ���������� ��� ������ item � detail, ����� ������������� �� ��������� �� ����� ������ ���������
    mdv.pk21_lock.LOCK_RESOURCE(p_Mode      => DBMS_LOCK.X_MODE,
                                p_Lock_Name => mdv.pk21_lock.c_Lock_Items);

    IF p_Data_Type IN ('SPB','NOVTK') THEN
       -- � ���������� � ����� ������ ������ �����
        l_Trf_Type := Get_List_BDR_Types(p_Data_Type,NULL,'D');
        
       -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
       -- ���������� order_body_t
       -- ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
       
       -- �������� ��� ������ ������� � ����������� order_body_id
        l_Insert := 0;
        l_Count  := 0;
       
        SELECT d.external_id
          INTO l_OB_Not_Found
          FROM PIN.DICTIONARY_T d
         WHERE LEVEL = 2
           AND KEY = 'OB_ID_ERR'  -- �� ������ order_BODY_ID
         CONNECT BY PRIOR d.key_id = d.parent_id
         START WITH d.KEY = 'TRF_STAT';
       
        OPEN l_Cursor FOR 'SELECT b.order_id, o.date_from, o.date_to, b.subservice_id, b.rateplan_id ' || CHR(10) ||
                          '  FROM bdr_oper_t b,' || CHR(10) ||
                          '       order_t o' || CHR(10) ||
                          ' WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To' || CHR(10) ||
                          '   AND b.bdr_type_id = :l_BDR_Type_Id ' || CHR(10) ||
                          '   AND b.trf_type IN (' || l_Trf_Type || ')' || CHR(10) ||
                          '   AND b.bdr_status = :l_Err_OB_NF' || CHR(10) ||
                          '   AND b.order_id = o.order_id         ' || CHR(10) ||   
                          '   AND (b.account_id = :l_Account_Id OR :l_Account_Id IS NULL)' || CHR(10) ||        
                          ' GROUP BY b.order_id, o.date_from, o.date_to, b.subservice_id, b.rateplan_id'
                          USING l_Date_From, l_Date_To, 
                                l_BDR_Type_Id,
                                l_OB_Not_Found,
                                p_Account_Id, p_Account_Id;
        
        LOOP
        
            FETCH l_Cursor INTO l_Order_Id, l_Ord_DFrom, l_Ord_DTo, l_Subservice_Id, l_Rateplan_Id;
                  
            EXIT WHEN l_Cursor%NOTFOUND;        
        
            BEGIN
               -- �������� ������ ��������� date_to � ��������
                IF l_Ord_DFrom > l_Ord_DTo THEN
                    l_Ord_DTo := pk00_const.c_Date_Max;
                END IF;
        
                l_Insert := pk06_order.Add_subservice (
                                           p_order_id      => l_Order_Id,
                                           p_subservice_id => l_Subservice_Id,
                                           p_charge_type   => pk00_const.c_CHARGE_TYPE_USG,
                                           p_rateplan_id   => l_Rateplan_Id,
                                           p_date_from     => l_Ord_DFrom,
                                           p_date_to       => l_Ord_DTo,
                                           p_notes         => '������ ������������� ��� ������������ ��������',
                                           p_currency_id   => Pk00_Const.c_CURRENCY_RUB
                                          );                 
            
                COMMIT;
             
                IF l_Insert < 0 THEN
                    Pk01_Syslog.write_Msg(p_Msg   => '������ ���������� ������ � order_body_t ��� �/�: ' || TO_CHAR(l_Order_Id) ||
                                                     ', ������: ' || TO_CHAR(l_Subservice_Id),                                           
                                          p_Src   => gc_PkgName || '.' || c_prcName,
                                          p_Level => pk01_syslog.L_err
                                          );    
                ELSE
                    l_Count := l_Count + 1;                          
                END IF;        
                
            EXCEPTION
                WHEN others THEN

                    Pk01_Syslog.write_Msg(p_Msg   => '������ ���������� ������ � order_body_t ��� �/�: ' || TO_CHAR(l_Order_Id) ||
                                                     ', ������: ' || TO_CHAR(l_Subservice_Id),                                           
                                          p_Src   => gc_PkgName || '.' || c_prcName,
                                          p_Level => pk01_syslog.L_err
                                          );    
                
            END;        
        
        END LOOP;                   
        
        CLOSE l_Cursor;                                                 
        
        IF l_Count > 0 THEN
            Pk01_Syslog.write_Msg(p_Msg   => '������� ������� � order_body_t: ' || TO_CHAR(l_Count), 
                                  p_Src   => gc_PkgName || '.' || c_prcName);
        END IF;                    
        
        -- ��������� ������ � BDR-��
        EXECUTE IMMEDIATE      
            'MERGE INTO (SELECT /*+ parallel(b 10) */ b.order_id, b.bdr_status, b.subservice_id, ' ||
            '                   b.local_time, b.order_body_id ' || CHR(10) ||
            '              FROM ' || l_BDR_Table || ' b ' || CHR(10) ||
            '             WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) || 
            '               AND b.bdr_type_id = :l_BDR_Type_Id ' || CHR(10) ||
            '               AND b.trf_type IN (' || l_Trf_Type || ')' || -- �����
            '               AND b.modify_date <= :l_Modify_Date ' || CHR(10) ||
            '               AND (b.account_id = :l_Account_Id OR :l_Account_Id IS NULL)' || CHR(10) ||
            '               AND b.bdr_status = :l_BDR_Status ' || CHR(10) || 
            '           ) b ' || CHR(10) ||
            'USING (SELECT order_body_id, order_id, subservice_id, date_from, date_to ' || CHR(10) ||
            '         FROM ORDER_BODY_T ' || CHR(10) || 
            '        WHERE charge_type = :l_Charge_Type ' || CHR(10) || 
            '      ) bl ' || CHR(10) ||
            '   ON (b.order_id = bl.order_id AND ' ||
            '       b.subservice_id = bl.subservice_id AND ' ||
            '       b.local_time BETWEEN bl.date_from AND NVL(bl.date_to,:l_Date_To)' ||
            '      ) ' || CHR(10) ||
            ' WHEN MATCHED THEN UPDATE ' || CHR(10) ||
            '  SET b.order_body_id = bl.order_body_id, ' || CHR(10) ||
            '      b.bdr_status = 0'
        USING l_Date_From, l_Date_To,
              l_BDR_Type_Id,
              l_Calc_Date,
              p_Account_Id, p_Account_Id,
              l_OB_Not_Found,
              pk00_const.c_CHARGE_TYPE_USG,
              gc_DATE_END;        
        
        Pk01_Syslog.write_Msg(p_Msg   => 'Set order_body_id to bdr: ' || TO_CHAR(SQL%ROWCOUNT) || 
                                         ', bdr_type: ' || TO_CHAR(l_BDR_Type_Id), 
                              p_Src   => gc_PkgName || '.' || c_prcName);        
        
    END IF;    
    
    
   -- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ 
   -- ��������/���������� ������� � bill_t ��� �/������, � ������� ��� ��� ������
   -- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
   
    l_Insert := 0;
    l_Count  := 0;
    
    OPEN l_Cursor FOR 'SELECT /*+ parallel(b 10) */ b.account_id ' ||
                      '  FROM ' || l_BDR_Table || ' b '   || CHR(10) ||
                      ' WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
                      '   AND b.bdr_type_id = :l_BDR_Type_Id ' || CHR(10) ||
                      '   AND b.bdr_status IN (0, :l_BDR_Status) ' || CHR(10) ||
                        (CASE WHEN p_Data_Type IN ('SPB','NOVTK') THEN 
                               ' AND b.trf_type IN (' || l_Trf_Type || ')' -- �����
                              ELSE NULL
                        END) ||                      
                      '   AND b.account_id IS NOT NULL ' || CHR(10) ||
                      '   AND (b.bill_id IS NULL OR b.bill_id < 0)' || CHR(10) ||
                      '   AND (b.account_id = :l_Account_Id OR :l_Account_Id IS NULL)' || CHR(10) ||
                      ' GROUP BY b.account_id ' || CHR(10) ||
                      ' HAVING NOT EXISTS (SELECT 1 ' || CHR(10) || -- ����� ����� ����� ��� ���� ������� ���� ������� ���  
                      '                      FROM bill_t bl ' || CHR(10) ||
                      '                     WHERE bl.rep_period_id = :l_Rep_Period_Id ' || CHR(10) ||
                      '                       AND bl.bill_type = :l_Bill_Type ' || CHR(10) ||
                      '                       AND bl.account_id = b.account_id)'
                  USING l_Date_From, l_Date_To,
                        l_BDR_Type_Id,
                        pk00_const.c_NO_BILL_FOUND, --c_BILL_NOT_FOUND,
                        p_Account_Id, p_Account_Id,
                        l_Rep_Period_Id, PK00_CONST.c_BILL_TYPE_REC;
    
    LOOP
    
        FETCH l_Cursor INTO l_External_Id;
              
        EXIT WHEN l_Cursor%NOTFOUND;

        BEGIN
            l_Insert := Pk07_Bill.Next_recuring_bill ( 
                                   p_account_id    => l_External_Id,   -- ID �������� ����� 
                                   p_rep_period_id => l_Rep_Period_Id    -- ID ���������� ������� YYYYMM 
                                 ); 
            COMMIT;
    
            IF l_Insert < 0 THEN
                Pk01_Syslog.write_Msg(p_Msg   => '������ ���������� ������ � bill_t ��� �/�: ' || TO_CHAR(l_External_Id) ||
                                                 ', ������: ' || TO_CHAR(l_Rep_Period_Id),                                           
                                      p_Src   => gc_PkgName || '.' || c_prcName,
                                      p_Level => pk01_syslog.L_err
                                      );    
            ELSE
                l_Count := l_Count + 1;                          
            END IF;
        
        EXCEPTION
            WHEN others THEN    
                Pk01_Syslog.write_Msg(p_Msg   => '������ ���������� ������ � bill_t ��� �/�: ' || TO_CHAR(l_External_Id) ||
                                                 ', ������: ' || TO_CHAR(l_Rep_Period_Id),                                           
                                      p_Src   => gc_PkgName || '.' || c_prcName,
                                      p_Level => pk01_syslog.L_err
                                      );                
        END;    

    END LOOP;
    
    CLOSE l_Cursor;     

    IF l_Count > 0 THEN
        Pk01_Syslog.write_Msg(p_Msg   => '������� ������: ' || TO_CHAR(l_Count), 
                              p_Src   => gc_PkgName || '.' || c_prcName);
    END IF;                           

   -- ����������� bdr-��, � ������� �� ���� ������, ����� bill_id
    EXECUTE IMMEDIATE      
        'MERGE INTO (SELECT /*+ parallel(b 10) */ b.bill_id, b.bdr_status, b.account_id ' || CHR(10) ||
        '              FROM ' || l_BDR_Table || ' b ' || CHR(10) ||
        '             WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) || 
        '               AND b.bdr_type_id = :l_BDR_Type_Id ' || CHR(10) ||
                        (CASE WHEN p_Data_Type IN ('SPB','NOVTK') THEN 
                               ' AND b.trf_type IN (' || l_Trf_Type || ')' -- �����
                              ELSE NULL
                        END) ||        
        '               AND b.modify_date <= :l_Modify_Date ' || CHR(10) ||
        '               AND (b.account_id = :l_Account_Id OR :l_Account_Id IS NULL)' || CHR(10) ||
        '               AND (b.bill_id IS NULL OR b.bill_id < 0) ' || CHR(10) ||
        '               AND b.bdr_status IN (0, :l_BDR_Status) ' || CHR(10) || 
        '           ) b ' || CHR(10) ||
        'USING (SELECT bill_id, account_id ' || CHR(10) ||
        '         FROM BILL_T bl ' || CHR(10) || 
        '        WHERE bl.rep_period_id = :l_Rep_Period_Id ' || CHR(10) || 
        '          AND bl.bill_status IN (:l_Open) ' || CHR(10) ||
        '          AND bl.bill_type = :l_Bill_Type ' || CHR(10) ||
        '      ) bl ' || CHR(10) ||
        '   ON (b.account_id = bl.account_id) ' || CHR(10) ||
        ' WHEN MATCHED THEN UPDATE ' || CHR(10) ||
        '  SET b.bill_id = bl.bill_id, ' || CHR(10) ||
        '      b.bdr_status = DECODE(b.bdr_status,1,0,b.bdr_status)'
    USING l_Date_From, l_Date_To,
          l_BDR_Type_Id,
          l_Calc_Date,
          p_Account_Id, p_Account_Id,
          pk00_const.c_NO_BILL_FOUND, --c_BILL_NOT_FOUND,
          l_Rep_Period_Id,
          PK00_CONST.c_BILL_STATE_OPEN,
          pk00_const.c_Bill_Type_Rec;

    Pk01_Syslog.write_Msg(p_Msg   => 'Set bill_id to bdr: ' || TO_CHAR(SQL%ROWCOUNT) || 
                                     ', bdr_type: ' || TO_CHAR(l_BDR_Type_Id), 
                          p_Src   => gc_PkgName || '.' || c_prcName);

  -- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  --  ������ item-��
  -- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
  
  -- �������� external id, ��� ������� ����� ������ � item_t
    l_External_Id := Get_External_Id(p_BDR_Type => p_Data_Type);

    -- ������� �� ����. ������
    DELETE FROM TMP_GROUP_ITEM;
    
    IF p_Data_Type IN ('SPB','NOVTK') THEN 
        EXECUTE IMMEDIATE
            'INSERT INTO TMP_GROUP_ITEM ' || CHR(10) ||
            '      (bill_id, item_id, account_id, order_id, order_body_id, service_id, subservice_id, ' || CHR(10) || 
            '       date_from, date_to, item_total, tax_incl, calls) ' || CHR(10) ||
            'SELECT /*+ parallel(b 10) */ ' ||
            '       b.bill_id, NVL(b.item_id, i.item_id), b.account_id, b.order_id, b.order_body_id, b.service_id, b.subservice_id, ' || CHR(10) ||  
            '       MIN(b.local_time), MAX(b.local_time), CEIL(SUM(b.amount)*100)/100, ' || -- ����� ��������� �� ������ �����
            '       ''N'' tax_incl, COUNT(1) calls ' || CHR(10) ||
            '  FROM ' || l_BDR_Table || ' b, ' || CHR(10) ||
            '       BILL_T bl, ' || CHR(10) ||            
            '       ITEM_T i ' || CHR(10) ||            
            ' WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
            '   AND b.bdr_type_id = :l_BDR_Type_Id ' || CHR(10) ||
            '   AND b.trf_type IN (' || l_Trf_Type || ')' || CHR(10) || -- �����
            '   AND b.modify_date <= :l_Modify_Date ' || CHR(10) ||
            '   AND b.bdr_status = 0 ' || CHR(10) ||
            '   AND b.bill_id > 0 ' || CHR(10) ||
            '   AND (b.account_id = :l_Account_Id OR :l_Account_Id IS NULL)' || CHR(10) ||
            '   AND b.bill_id = bl.bill_id ' || CHR(10) ||
            '   AND bl.rep_period_id = :l_Rep_Period_Id ' || CHR(10) ||
            '   AND bl.bill_status IN (:l_Open) ' || CHR(10) ||     
            '   AND i.charge_type(+) = :l_USG ' || CHR(10) ||      
            '   AND i.rep_period_id(+) = :l_Rep_Period_Id ' || CHR(10) ||
            '   AND i.external_id(+) = :l_External_Id ' || CHR(10) ||
            '   AND i.bill_id(+) = b.bill_id ' || CHR(10) ||            
            '   AND i.order_id(+) = b.order_id ' || CHR(10) ||
            '   AND i.service_id(+) = b.service_id ' || CHR(10) ||
            '   AND NVL(i.subservice_id(+), -1) = NVL(b.subservice_id,-1) ' || CHR(10) ||
            '   AND TRUNC(i.date_from(+),''mm'') = TRUNC(b.local_time,''mm'') ' || CHR(10) ||            
            ' GROUP BY b.bill_id,  NVL(b.item_id, i.item_id), b.account_id, b.order_id, b.order_body_id, ' || CHR(10) || 
            '          b.service_id, b.subservice_id, TRUNC(b.local_time,''mm'')'
        USING l_Date_From, l_Date_To, 
              l_BDR_Type_Id,
              l_Calc_Date,
              p_Account_Id, p_Account_Id,
              l_Rep_Period_Id,
              PK00_CONST.c_BILL_STATE_OPEN,
              pk00_const.c_CHARGE_TYPE_USG,              
              l_Rep_Period_Id, l_External_Id;
              
        --INSERT INTO PIN.MS_TMP_GROUP_ITEM
        --SELECT * FROM PIN.TMP_GROUP_ITEM;
        --COMMIT; RAISE no_data_found;                
              
    ELSE

        EXECUTE IMMEDIATE   
            'INSERT INTO TMP_GROUP_ITEM ' || CHR(10) ||
            '      (bill_id, item_id, account_id, order_id, order_body_id, service_id, subservice_id, ' || CHR(10) ||  
            '       date_from, date_to, item_total, tax_incl, calls) ' || CHR(10) ||
            'SELECT /*+ parallel(b 5) */ ' || CHR(10) ||
            '       b.bill_id, NVL(b.item_id, i.item_id), b.account_id, b.order_id, b.order_body_id, b.service_id, b.subservice_id, ' || CHR(10) ||   
            '       MIN(b.local_time), MAX(b.local_time), CEIL(SUM(b.amount)*100)/100, r.tax_incl, COUNT(1) calls ' || CHR(10) ||
            '  FROM ' || l_BDR_Table || ' b, ' || CHR(10) ||
            '       TARIFF_PH.D41_TRF_HEADER h, ' || CHR(10) || 
            '       PIN.RATEPLAN_T r, ' || CHR(10) ||
            '       BILL_T bl, ' || CHR(10) ||
            '       ITEM_T i ' || CHR(10) ||
            ' WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) || 
            '   AND b.bdr_type_id = :l_BDR_Type_Id ' || CHR(10) ||
            '   AND b.modify_date <= :l_Modify_Date ' || CHR(10) || 
            '   AND b.bdr_status = 0 ' || CHR(10) ||
            '   AND (b.account_id = :l_Account_Id OR :l_Account_Id IS NULL) ' || CHR(10) ||
            '   AND b.trf_id = h.trf_id ' || CHR(10) ||
            '   AND h.code = r.rateplan_code ' || CHR(10) ||
            '   AND b.bill_id = bl.bill_id ' || CHR(10) ||
            '   AND bl.rep_period_id = :l_Rep_Period_Id ' || CHR(10) ||
            '   AND bl.bill_status IN (:l_Open) ' || CHR(10) ||
            '   AND i.charge_type(+) = :l_USG ' || CHR(10) ||
            '   AND i.bill_id(+) = b.bill_id ' || CHR(10) ||
            '   AND i.rep_period_id(+) = :l_Rep_Period_Id ' || CHR(10) ||
            '   AND i.external_id(+) = :l_External_Id ' || CHR(10) ||
            '   AND i.bill_id(+) = b.bill_id ' || CHR(10) ||            
            '   AND i.order_id(+) = b.order_id ' || CHR(10) ||
            '   AND i.order_body_id(+) = b.order_body_id ' || CHR(10) ||
            '   AND i.service_id(+) = b.service_id ' || CHR(10) ||
            '   AND NVL(i.subservice_id(+), -1) = NVL(b.subservice_id,-1) ' || CHR(10) ||
            '   AND TRUNC(i.date_from(+),''mm'') = TRUNC(b.local_time,''mm'') ' || CHR(10) ||
            ' GROUP BY b.bill_id, NVL(b.item_id, i.item_id), b.account_id, b.order_id, b.order_body_id, ' || CHR(10) ||  
            '          b.service_id, b.subservice_id, TRUNC(b.local_time,''mm''), r.tax_incl'   
        USING l_Date_From, l_Date_To, 
              l_BDR_Type_Id,
              l_Calc_Date,
              p_Account_Id, p_Account_Id,
              l_Rep_Period_Id,
              PK00_CONST.c_BILL_STATE_OPEN,
              pk00_const.c_CHARGE_TYPE_USG,
              l_Rep_Period_Id,
              l_External_Id;    
    
    END IF;         

   -- ����������� ����� item_id ��� �������, � ������� �� ��� ���
    UPDATE TMP_GROUP_ITEM
       SET item_id = PK02_POID.Next_Item_Id,
           flag_new = 'Y'
     WHERE item_id IS NULL
       AND bill_id IS NOT NULL;   


--INSERT INTO MS_TMP_GROUP_ITEM SELECT * FROM TMP_GROUP_ITEM;
--COMMIT;
--RAISE no_data_found;

   -- ��������� ���� item_id � BDR-��, � ������� ��� ����� �������� 
    EXECUTE IMMEDIATE
        'UPDATE /*+ parallel(b 5) */ ' || l_BDR_Table || ' b ' || CHR(10) ||
        '   SET b.item_id = (SELECT i.item_id ' || CHR(10) ||
        '                      FROM TMP_GROUP_ITEM i ' || CHR(10) ||
        '                     WHERE i.bill_id       = b.bill_id ' || CHR(10) ||
        '                       AND i.order_id      = b.order_id ' || CHR(10) ||
        '                       AND i.order_body_id = b.order_body_id ' || CHR(10) ||
        '                       AND i.service_id    = b.service_id ' || CHR(10) ||
        '                       AND NVL(i.subservice_id,-1) = NVL(b.subservice_id,-1) ' || CHR(10) || 
        '                       AND TRUNC(b.local_time,''mm'') = TRUNC(i.date_from,''mm'')' ||
        '                    ), ' || CHR(10) ||
        '       b.item_date = :l_Calc_Date ' || CHR(10) ||
        ' WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
        '   AND b.bdr_type_id = :l_BDR_Type_Id ' || CHR(10) ||
        (CASE WHEN p_Data_Type IN ('SPB','NOVTK') THEN 
               ' AND b.trf_type IN (' || l_Trf_Type || ')' -- �����
              ELSE NULL
        END) ||
        '   AND b.modify_date <= :l_Modify_Date ' || CHR(10) ||
        '   AND b.bdr_status = 0 ' || CHR(10) ||      
        '   AND (b.item_id IS NULL ' || 
                           'OR :l_Account_Id IS NOT NULL) ' || CHR(10) || -- ����� �������� �/�����
        '   AND b.bill_id IS NOT NULL' || CHR(10) ||
        '   AND (b.account_id = :l_Account_Id OR :l_Account_Id IS NULL)'
    USING l_Calc_Date,
          l_Date_From, l_Date_To, 
          l_BDR_Type_Id,
          l_Calc_Date,
          p_Account_Id,
          p_Account_Id, p_Account_Id;
                         
    l_Count := SQL%ROWCOUNT;      
          
    
/*    IF p_Data_Type NOT IN ('SPB','NOVTK') THEN
    
        l_Moved := 0;
    
      -- �������� ������� ���������, ��� ����������� ������ �� ���������
        FOR l_cur IN (SELECT TRUNC(t.date_from,'mm') calc_month
                        FROM TMP_GROUP_ITEM t
                       WHERE t.date_to < l_Date_From -- l_Date_From - ���� ��������� �������.
                       GROUP BY TRUNC(t.date_from,'mm')
                       ORDER BY calc_month
                     )   
        LOOP
    
            l_Prev_From := l_cur.calc_month;
            l_Prev_To   := LAST_DAY(l_cur.calc_month) + INTERVAL '00 23:59:59' DAY TO SECOND;
    
            DBMS_APPLICATION_INFO.SET_ACTION('Upd. MONTH: ' || TO_CHAR(l_cur.calc_month,'dd.mm.yyyy'));
    
           -- ������ ������ ���������, �.�. ����������� item_id bdr-��, ������� ��������� � ����� �������,
           -- �� � ����� ������ � �����������. ��� ���� ��� ������ ���������� � ������ ������ ��� �����������
            EXECUTE IMMEDIATE  
                'MERGE INTO bdr_voice_t b ' || CHR(10) || */
    --            'USING (SELECT /*+ parallel(b 5)*/ b.rowid rd, t.item_id ' || CHR(10) ||
   /*            '         FROM TMP_GROUP_ITEM t, ' || CHR(10) ||
                               l_BDR_Table || ' b '|| CHR(10) ||
                '        WHERE b.item_id IS NULL ' || CHR(10) ||
                '          AND b.bdr_type_id = :l_BDR_Type_Id ' || CHR(10) ||                  
                '          AND b.rep_period BETWEEN :l_date_from AND :l_date_to ' || CHR(10) ||
                '          AND b.rep_period BETWEEN t.date_from AND t.date_to ' || CHR(10) ||
                '          AND TRUNC(b.rep_period,''mm'') != TRUNC(t.date_from,''mm'') ' || CHR(10) ||   
                '          AND b.order_id   = t.order_id ' || CHR(10) ||   
                '          AND b.service_id = t.service_id ' || CHR(10) || 
                '          AND NVL(b.subservice_id,-1) = NVL(t.subservice_id,-1) ' || CHR(10) || 
                '          AND TRUNC(b.local_time,''mm'') = TRUNC(t.date_from,''mm'') ' || CHR(10) ||       
                '      ) i ' || CHR(10) ||
                '   ON (b.rowid = i.rd) ' || CHR(10) ||  
                'WHEN MATCHED THEN UPDATE ' || CHR(10) ||       
                '  SET b.item_id = i.item_id, ' || CHR(10) ||
                '      b.item_date = :l_Calc_Date'
            USING l_BDR_Type_Id, l_Prev_From, l_Prev_To, 
                  l_Calc_Date;
            
            l_Moved := l_Moved + SQL%ROWCOUNT;       
            
        END LOOP;    
    END IF;    */

/*    BEGIN
            SELECT rd 
              INTO l_RowId
              FROM (
                         SELECT i.rowid rd   
                         FROM TMP_GROUP_ITEM t,
                              ITEM_T i
                        WHERE i.rep_period_id = l_Rep_Period_Id
                          AND i.item_id = t.item_id
                          AND (
                               p_Account_Id IS NOT NULL -- �������������� �������� �/�����
                                OR
                               i.item_status = pk00_const.c_ITEM_STATE_ERROR
                                OR 
                               i.item_total != t.item_total 
                                OR 
                               NVL(i.date_from,gc_DATE_END)  != NVL(t.date_from,gc_DATE_END)
                                OR 
                               NVL(i.date_to,gc_DATE_END)    != NVL(t.date_to,gc_DATE_END)  
                              )             
                     GROUP BY i.rowid 
                     HAVING COUNT(1) > 1
                  )   
            WHERE ROWNUM = 1;
    
            Pk01_Syslog.write_Msg(p_Msg   => 'DEBUG: ' || ROWIDTOCHAR(l_RowId), 
                                  p_Src   => gc_PkgName || '.' || c_prcName);    
    
    EXCEPTION
        WHEN no_data_found THEN
            Pk01_Syslog.write_Msg(p_Msg   => 'DEBUG: dbl NOT FOUND', 
                                  p_Src   => gc_PkgName || '.' || c_prcName);        
        
    END;        */
                
   -- ������ ��������� � item-�, � ������� ��� �������        
    MERGE INTO ITEM_T i
    USING (SELECT i.rowid rd, t.item_total, t.date_from, t.date_to, t.calls 
             FROM TMP_GROUP_ITEM t,
                  ITEM_T i
            WHERE i.rep_period_id = l_Rep_Period_Id
              AND i.item_id = t.item_id
              AND (
                   p_Account_Id IS NOT NULL -- �������������� �������� �/�����
                    OR
                   i.item_status = pk00_const.c_ITEM_STATE_ERROR
                    OR 
                   i.item_total != t.item_total 
                    OR 
                   NVL(i.date_from,gc_DATE_END)  != NVL(t.date_from,gc_DATE_END)
                    OR 
                   NVL(i.date_to,gc_DATE_END)    != NVL(t.date_to,gc_DATE_END)  
                    OR
                   NVL(i.quantity, -1) != t.calls 
                  )         
          ) t        
       ON (i.rowid = t.rd)
    WHEN MATCHED THEN UPDATE
     SET  i.item_total    = t.item_total, 
          i.date_from     = t.date_from,
          i.date_to       = t.date_to,
          i.last_modified = l_Calc_Date,
          i.item_status   = pk00_const.c_ITEM_STATE_OPEN,
          i.quantity      = t.calls;        
               
    l_Update := SQL%ROWCOUNT;      
          
   -- ��������� ����� ������ �����
    INSERT INTO PIN.ITEM_T (
           REP_PERIOD_ID, BILL_ID, ITEM_ID, ITEM_TYPE, --INV_ITEM_ID, 
           ORDER_ID, ORDER_BODY_ID, SERVICE_ID, SUBSERVICE_ID, CHARGE_TYPE, 
           ITEM_TOTAL, QUANTITY, --ADJUSTED, TRANSFERED, RECVD, DUE, 
           DATE_FROM, DATE_TO, ITEM_STATUS, TAX_INCL, EXTERNAL_ID,
           CREATE_DATE, LAST_MODIFIED)
    SELECT l_Rep_Period_Id, t.bill_id, t.item_id, pk00_const.c_ITEM_TYPE_BILL, --NULL,
           t.order_id, t.order_body_id, t.service_id, t.subservice_id, pk00_const.c_CHARGE_TYPE_USG,
           t.item_total, t.calls,
           t.date_from, t.date_to, pk00_const.c_ITEM_STATE_OPEN, t.tax_incl, l_External_Id,
           l_Calc_Date, l_Calc_Date
      FROM TMP_GROUP_ITEM t
     WHERE t.flag_new = 'Y'
        OR NOT EXISTS (SELECT 1
                         FROM item_t i
                        WHERE i.rep_period_id = l_Rep_Period_Id
                          AND i.item_id = t.item_id);    
          
    l_Insert := SQL%ROWCOUNT;  
     
   -- ������� item-�, ��� ������� ��� ��� BDR-��
    DELETE FROM item_t i
     WHERE i.rep_period_id = l_Rep_period_Id
       AND i.external_id = l_External_Id
       AND (p_Account_Id IS NULL 
              OR
            i.order_id IN (SELECT o.order_id
                             FROM order_t o
                            WHERE o.account_id = p_Account_Id)
           )                   
       AND EXISTS (SELECT 1
                     FROM bill_t b
                    WHERE b.rep_period_id = l_Rep_Period_Id
                      AND b.bill_status IN (pk00_const.c_BILL_STATE_OPEN)
                      AND b.bill_id = i.bill_id)
       AND NOT EXISTS (SELECT 1
                         FROM TMP_GROUP_ITEM t
                        WHERE t.item_id = i.item_id);       
           
    l_Delete := SQL%ROWCOUNT;   

     
    Pk01_Syslog.write_Msg(p_Msg   => 'Recal. items. ' ||
                                     'Period ' || TO_CHAR(p_period, 'dd.mm.yyyy') ||
                                     ': Set item_id to bdr: ' || TO_CHAR(l_Count) ||
                                        ' (��������: ' || TO_CHAR(NVL(l_Moved,0)) || ') ' ||
                                     ', ins: ' || TO_CHAR(l_Insert) ||
                                     ', dlt: ' || TO_CHAR(l_Delete) || 
                                     ', upd: ' || TO_CHAR(l_Update),
                          p_Src   => gc_PkgName || '.' || c_prcName);
    
    
    IF p_Data_Type NOT IN ('SPB','NOVTK') THEN
    
       -- +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
       -- ���������� �� ������������ �������        
       
        IF l_Delete > 0 OR l_Insert > 0 OR l_Update > 0 THEN
        
          --- +++++ ����������� ���� ++++++
        
           -- ������� ������, ������� ���� �������� ��� ������� ��� ���
            DELETE FROM PIN.DETAIL_MMTS_T_JUR d
             WHERE d.rep_period_id = l_Rep_Period_Id
               AND (p_Account_Id IS NULL 
                      OR
                    d.order_id IN (SELECT o.order_id
                                     FROM order_t o
                                    WHERE o.account_id = p_Account_Id)
                   )                  
               AND (NOT EXISTS (SELECT 1
                                  FROM ITEM_T i
                                 WHERE i.rep_period_id = l_Rep_Period_Id
                                   AND i.item_id = d.item_id)
                    OR
                    EXISTS (SELECT 1
                              FROM ITEM_T i
                             WHERE i.rep_period_id = l_Rep_Period_Id
                               AND i.last_modified = l_Calc_Date                         
                               AND i.item_id = d.item_id)
                   );
           
            l_Delete := SQL%ROWCOUNT;
           
           -- ��������� ����� ������
            EXECUTE IMMEDIATE
                'INSERT INTO PIN.DETAIL_MMTS_T_JUR ( ' || CHR(10) ||
                '         REP_PERIOD_ID, ' || CHR(10) ||
                '         BILL_ID, ' || CHR(10) ||
                '         ITEM_ID, ' || CHR(10) ||
                '         ORDER_ID, ' || CHR(10) ||
                '         ORDER_NO, ' || CHR(10) ||
                '         SERVICE_ID, ' || CHR(10) ||
                '         SUBSERVICE_ID, ' || CHR(10) ||
                '         ABN_A, ' || CHR(10) ||
                '         PREFIX_B, ' || CHR(10) ||
                '         TERM_Z_NAME, ' || CHR(10) ||
                '         CALLS, ' || CHR(10) ||
                '         MINUTES, ' || CHR(10) ||
                '         TOTAL, ' || CHR(10) ||
                '         SUBSERVICE_KEY ' || CHR(10) ||
                '      ) ' || CHR(10) ||
                'SELECT /*+ parallel(b 5) */ i.rep_period_id, ' || CHR(10) ||
                '       i.bill_id, ' || CHR(10) || 
                '       i.item_id, ' || CHR(10) || 
                '       b.order_id, ' || CHR(10) ||
                '       o.order_no, ' || CHR(10) ||
                '       b.service_id, ' || CHR(10) ||
                '       b.subservice_id, ' || CHR(10) ||
                '       DECODE(b.subservice_id, 8, b.abn_f, b.abn_a) abn_a, ' || CHR(10) || -- ��� 8800 � ����� �������� � ������ 8800XXXXX ' || CHR(10) ||
                '       b.prefix_b, ' || CHR(10) ||
                '       b.term_z_name, ' || CHR(10) || 
                '       COUNT(1) calls, ' || CHR(10) ||
                '       SUM(b.bill_minutes), ' || CHR(10) ||
                '       SUM(b.amount), ' || CHR(10) || 
                '       ss.subservice_key ' || CHR(10) || 
                '  FROM ' || l_BDR_Table || ' b, ' || CHR(10) ||
                '       PIN.ITEM_T i, ' || CHR(10) ||
                '       PIN.ORDER_T o, ' || CHR(10) ||
                '       PIN.ACCOUNT_T a, ' || CHR(10) ||
                '       PIN.BILL_T bl, ' || CHR(10) ||
                '       PIN.SUBSERVICE_T ss ' || CHR(10) ||                
                ' WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
                '   AND b.bdr_status = 0 ' || CHR(10) ||
                '   AND (b.account_id = :l_Account_Id OR :l_Account_Id IS NULL)' || CHR(10) ||
                '   AND i.rep_period_id = :l_Rep_Period_Id ' || CHR(10) ||
                '   AND i.last_modified = :l_Calc_Date ' || CHR(10) || 
                '   AND b.item_id = i.item_id ' || CHR(10) ||
               -- '   AND b.bill_id = i.bill_id ' || CHR(10) ||
                '   AND i.order_id = o.order_id ' || CHR(10) ||
                '   AND a.account_type = ''J'' ' || CHR(10) || 
                '   AND b.account_id = a.account_id ' || CHR(10) ||
                '   AND i.bill_id = bl.bill_id ' || CHR(10) ||
                '   AND b.subservice_id = ss.subservice_id(+) ' || CHR(10) ||
                ' GROUP BY i.rep_period_id, i.bill_id, i.item_id, b.order_id, o.order_no, ' || CHR(10) ||
                '          b.service_id, b.subservice_id, ' ||
                '          DECODE(b.subservice_id, 8, b.abn_f, b.abn_a), ' || CHR(10) || -- ��� 8800 � ����� �������� � ������ 8800XXXXX
                '          b.prefix_b, b.term_z_name, ss.subservice_key'
            USING --p_Period,
                  l_Date_From, l_Date_To,
                  p_Account_Id, p_Account_Id,
                  l_Rep_Period_Id, l_Calc_Date;
                      
            l_Insert := SQL%ROWCOUNT;           
            
            Pk01_Syslog.write_Msg(p_Msg   => 'Loaded to Detail (Yur) ' ||
                                             'Period ' || TO_CHAR(p_period, 'dd.mm.yyyy') ||
                                             ': ins: ' || TO_CHAR(l_Insert) ||
                                             ', del: ' || TO_CHAR(l_Delete), 
                                  p_Src   => gc_PkgName || '.' || c_prcName);
                      
           --- ++++ ���������� ���� ++++++
           
           -- ������� ������, ������� ���� �������� ��� �������
            DELETE FROM PIN.DETAIL_MMTS_T_FIZ d
             WHERE d.rep_period_id = l_Rep_Period_Id
               AND (d.account_id = p_Account_Id OR p_Account_Id IS NULL)
               AND (NOT EXISTS (SELECT 1
                                  FROM ITEM_T i
                                 WHERE i.rep_period_id = l_Rep_Period_Id
                                   AND i.item_id = d.item_id
                               )
                    OR
                    EXISTS (SELECT 1
                              FROM ITEM_T i
                             WHERE i.rep_period_id = l_Rep_Period_Id
                               AND i.last_modified = l_Calc_Date
                               AND i.item_id = d.item_id
                           )
                   );   
            
            l_Delete := SQL%ROWCOUNT;
            
           -- ��������� ����� ������
            EXECUTE IMMEDIATE
                'INSERT INTO PIN.DETAIL_MMTS_T_FIZ ( ' || CHR(10) ||
                '         REP_PERIOD_ID, ' || CHR(10) ||
                '         ACCOUNT_ID, ' || CHR(10) ||
                '         PREFIX_B, ' || CHR(10) ||
                '         TERM_Z_NAME, ' || CHR(10) ||
                '         CALL_DAY, ' || CHR(10) ||
                '         CALLS_COUNT, ' || CHR(10) ||
                '         MINS_SUM, ' || CHR(10) ||
                '         AMOUNT_SUM, ' || CHR(10) ||
                '         SUBSERVICE_KEY, ' || CHR(10) ||
                '         ITEM_ID ' || CHR(10) ||
                '      ) ' || CHR(10) ||
                'SELECT  /*+ parallel(b 5) */ ' ||
                '       i.rep_period_id, ' || CHR(10) ||
                '       b.account_id, ' || CHR(10) ||
                '       b.prefix_b, ' || CHR(10) ||
                '       b.term_z_name, ' || CHR(10) ||
                '       TRUNC(b.local_time,''dd''), ' || CHR(10) || 
                '       COUNT(1) calls, ' || CHR(10) ||
                '       SUM(b.bill_minutes), ' || CHR(10) ||
                '       SUM(b.amount), ' || CHR(10) || 
                '       ss.subservice_key, ' || CHR(10) ||
                '       b.item_id ' || CHR(10) || 
                '  FROM ' || l_BDR_Table || ' b, ' || CHR(10) ||
                '       PIN.ACCOUNT_T a, '|| CHR(10) ||
                '       PIN.SUBSERVICE_T ss, ' || CHR(10) ||
                '       PIN.ITEM_T i ' || CHR(10) ||                
                ' WHERE b.rep_period BETWEEN :l_Date_From AND :l_Date_To ' || CHR(10) ||
                '   AND b.bdr_status = 0 ' || CHR(10) ||
                '   AND (b.account_id = :l_Account_Id OR :l_Account_Id IS NULL)' || CHR(10) ||
                '   AND a.account_type = ''P'' ' || CHR(10) ||
                '   AND b.account_id = a.account_id ' || CHR(10) || 
                '   AND b.subservice_id = ss.subservice_id(+) ' || CHR(10) ||
                '   AND i.rep_period_id = :l_Rep_Period_Id ' || CHR(10) ||
                '   AND i.last_modified = :l_Calc_Date ' || CHR(10) ||          
                '   AND b.item_id = i.item_id ' || CHR(10) ||   
             --   '   AND b.bill_id = i.bill_id ' || CHR(10) ||                    
                ' GROUP BY i.rep_period_id, b.item_id, b.account_id, ' || CHR(10) ||
                '          b.prefix_b, b.term_z_name, TRUNC(b.local_time,''dd''), ss.subservice_key'
            USING --p_Period,
                  l_Date_From, l_Date_To,
                  p_Account_Id, p_Account_Id,
                  l_Rep_Period_Id, l_Calc_Date;
                          
            l_Insert := SQL%ROWCOUNT;           

            Pk01_Syslog.write_Msg(p_Msg   => 'Loaded to Detail (Fiz) ' ||
                                             'Period ' || TO_CHAR(p_period, 'dd.mm.yyyy') ||
                                             ': ins: ' || TO_CHAR(l_Insert) ||
                                             ', del: ' || TO_CHAR(l_Delete), 
                                  p_Src   => gc_PkgName || '.' || c_prcName);   

        END IF;                      

    END IF;

    COMMIT;
    
    mdv.pk21_lock.UNLOCK_RESOURCE(p_Lock_Name => mdv.pk21_lock.c_Lock_Items);    

/*    
EXCEPTION
    WHEN others THEN
        mdv.pk21_lock.UNLOCK_RESOURCE(p_Lock_Name => mdv.pk21_lock.c_Lock_Items);
        RAISE; */ 

END Load_BDR_to_Item;


-- ++++++++++++++++++++++++++++++++
-- ������ ��������� ����������
----
PROCEDURE Load_Op_MinPay(p_Data_Type  IN varchar2,
                         p_Rep_Period IN date,
                         p_Call_Month IN date DEFAULT NULL
                        ) 
IS
    c_prcName       CONSTANT varchar2(16) := 'Load_Op_MinPay';      
    
    c_Network_NF    CONSTANT number := 0;
    
    l_Calc_Date     date := SYSDATE;
    l_BDR_Table     varchar2(32);

    l_Rep_Period_Id number;
    l_External_Id   number;
    l_BDR_Type      number;
    l_R_Date_From   date;
    l_R_Date_To     date;
    l_Date_From     date;
    l_Date_To       date;
    l_Count         number;
    l_Merged        number;
    l_Delete        number;    
    l_Items         number;
BEGIN
      
    l_Rep_Period_Id := pk00_const.Get_Period_Id(p_Rep_Period);
    l_External_Id   := Get_External_Id(p_BDR_Type => p_Data_Type);
    
   -- �������� ��� �������
    l_BDR_Type := PIN.Get_BDR_Type(p_Data_Type => UPPER(p_Data_Type),
                                   p_BDR_Table => l_BDR_Table, -- out
                                   p_Agent     => l_Merged,    -- out (����� �� ������������)
                                   p_Items     => l_Items
                                  );    
    
    IF l_Items = 0 THEN
    
        Pk01_Syslog.write_Msg(p_Msg   => p_Data_Type || ' �� �������� ����������� � item_t.', 
                              p_Src   => gc_PkgName || '.' || c_prcName,
                              p_Level => Pk01_Syslog.L_err);
                              
        RETURN;                    
            
    END IF;
    
    -- ���������� ���� �������, ��� ����� ������
    l_R_Date_From := TRUNC(p_Rep_Period,'mm');
    l_R_Date_To   := LAST_DAY(TRUNC(p_Rep_Period,'mm')) + INTERVAL '00 23:59:59' DAY TO SECOND;    
    
    -- ���������� ������ � ����� �������, ���������� �� ������� ��������������
    IF p_Call_Month IS NOT NULL THEN
        l_Date_From := TRUNC(p_Call_Month,'mm');
        l_Date_To   := LAST_DAY(TRUNC(p_Call_Month,'mm')) + INTERVAL '00 23:59:59' DAY TO SECOND;
    ELSE
        l_Date_From := l_R_Date_From;
        l_Date_To   := l_R_Date_To;
    END IF;               

    Pk01_Syslog.write_Msg(p_Msg => 'Calc. minim. pay ' ||
                                     'Period ' || TO_CHAR(p_Rep_Period, 'dd.mm.yyyy') ||
                                     ', Date: ' || TO_CHAR(l_Date_From, 'dd.mm.yyyy'),
                          p_Src => gc_PkgName || '.' || c_prcName);

   -- ������� ��������� �������
    DELETE FROM PIN.TMP11_MIN_CALC;

    INSERT INTO PIN.TMP11_MIN_CALC (
           BILL_ID, ACCOUNT_ID, ORDER_ID, SERVICE_ID, SUBSERVICE_ID, PRICE,
           TAX_INCL, REC_ID, OP_RATE_PLAN_ID, SUMMA,
           NETWORK_ID) 
    SELECT bl.bill_id, o.account_id, o.order_id, o.service_id, g.srv_id, p.price,   
           r.tax_incl, g.rec_id, g.op_rate_plan_id, g.summa,
           NVL(oi.network_id, c_Network_NF) -- ��� �������� ����� ��������� � ���� item_t.external_id ���� � ������ ��� BDR-��    
      FROM PIN.x07_ord_gv_t g, 
           pin.x07_ord_price_t p, 
           PIN.X07_OP_RATE_PLAN rp,  
           PIN.RATEPLAN_T r, 
           PIN.ORDER_T o, 
           PIN.ORDER_INFO_T oi,
           PIN.BILL_T bl
     WHERE g.summa > 0 
       AND l_Date_From BETWEEN g.date_from AND NVL(g.date_to, gc_DATE_END)  
       AND l_Date_From BETWEEN p.date_from AND NVL(p.date_to, gc_DATE_END)  
       AND g.op_rate_plan_id = p.op_rate_plan_id 
       AND g.srv_id = p.srv_id 
       AND g.op_rate_plan_id = rp.op_rate_plan_id 
       AND rp.rateplan_id = r.rateplan_id 
       AND rp.rateplan_id = o.rateplan_id 
       AND l_Date_From BETWEEN o.date_from AND NVL(o.date_to, gc_DATE_END)
       AND o.order_id = oi.order_id(+)  
       AND bl.bill_status IN (PK00_CONST.c_BILL_STATE_OPEN)  
       AND bl.rep_period_id = l_Rep_Period_Id  
       AND bl.account_id = o.account_id 
     GROUP BY bl.bill_id, o.account_id, o.order_id, o.service_id, oi.network_id,
              g.srv_id, p.price, r.tax_incl, g.rec_id, g.op_rate_plan_id, g.summa;

    l_Count := SQL%ROWCOUNT;
    
    UPDATE PIN.TMP11_MIN_CALC c
       SET order_body_id = (SELECT b.order_body_id
                              FROM pin.order_body_t b
                             WHERE b.order_id = c.order_id
                               AND b.charge_type = pk00_const.c_CHARGE_TYPE_USG
                               AND b.subservice_id = c.subservice_id
                               AND l_Date_From BETWEEN b.date_from AND b.date_to
                            ); 
    
    EXECUTE IMMEDIATE
        'MERGE INTO item_t i ' || CHR(10) ||
        'USING ( ' || CHR(10) ||
        '       SELECT  r.external_id, :l_Item_Type item_type, :l_Item_Status item_status, ' ||
                      ' :l_Charge_Type charge_type, ' || CHR(10) || 
        '               r.bill_id, r.order_id, r.order_body_id, r.service_id, r.subservice_id, ' || CHR(10) || 
        '               ROUND((r.summa - NVL(r.bill_minutes,0)) * r.price,2) item_total, ' || CHR(10) || 
        '               :l_Date_From date_from, :l_Date_To date_to, r.tax_incl ' || CHR(10) ||
        '          FROM (SELECT m.bill_id, m.order_id, NVL(b.order_body_id, m.order_body_id) order_body_id, ' || CHR(10) ||  
        '                       m.service_id, m.subservice_id, m.summa, b.bill_minutes, m.price, m.tax_incl, ' || CHR(10) ||
        '                       NVL(b.bdr_type_id, m.network_id) external_id ' || CHR(10) ||
        '                  FROM PIN.TMP11_MIN_CALC m, ' || CHR(10) ||
        '                       ( ' || CHR(10) ||   
        '                        SELECT /*+ parallel(b 10) */' || CHR(10) || 
        '                               b.bill_id,  b.bdr_type_id, b.account_id, b.order_id, b.order_body_id, ' || CHR(10) || 
        '                               b.service_id, b.subservice_id, b.op_rate_plan_id, ' || CHR(10) || 
        '                               TRUNC(b.local_time,''mm'') calc_month, SUM(b.bill_minutes) bill_minutes ' || CHR(10) ||
        '                          FROM ' || l_Bdr_Table || ' b ' || CHR(10) ||
        '                         WHERE b.rep_period BETWEEN :l_r_date_from AND :l_r_date_to ' || CHR(10) || 
        '                           AND b.local_time BETWEEN :l_date_from AND :l_date_to ' || CHR(10) ||
        '                           AND b.bdr_status = 0 ' || CHR(10) || -- ������� ������������������� ����������
       -- '                           AND b.bdr_type_id = :l_Bdr_Type_Id ' || CHR(10) ||
        '                           AND b.trf_type IN (' || Get_List_BDR_Types(p_Data_Type => p_Data_Type,
                                                                               p_Side     => NULL,
                                                                               p_In_Out   => 'D'  -- D - �����, � - ������
                                                                              ) || ') ' || CHR(10) ||
        '                         GROUP BY b.bill_id, b.bdr_type_id, b.account_id, b.order_id, b.order_body_id, ' || CHR(10) || 
        '                                  b.service_id, b.subservice_id, b.op_rate_plan_id, TRUNC(b.local_time,''mm'') ' || CHR(10) ||
        '                       ) b ' || CHR(10) ||
        '                 WHERE b.bill_id(+) = m.bill_id ' || CHR(10) || 
        '                   AND b.order_id(+) = m.order_id ' || CHR(10) ||
        '                   AND b.op_rate_plan_id(+) = m.op_rate_plan_id ' || CHR(10) ||
        '                   AND b.subservice_id(+) = m.subservice_id ' || CHR(10) ||
        '               ) r ' || CHR(10) ||
        '         WHERE r.summa - NVL(r.bill_minutes,0) > 0 ' || CHR(10) || -- ������ ��� ��� ����������          
        '      ) n ' || CHR(10) ||
        ' ON (i.rep_period_id = :l_Rep_Period_Id AND ' || CHR(10) || 
        '     i.external_id   = n.external_id    AND ' || CHR(10) ||
        '     i.item_type     = n.item_type      AND ' || CHR(10) ||
        '     i.item_status   = n.item_status    AND ' || CHR(10) ||
        '     i.charge_type   = n.charge_type    AND ' || CHR(10) ||
        '     i.bill_id       = n.bill_id        AND ' || CHR(10) ||
        '     i.order_id      = n.order_id       AND ' || CHR(10) ||
        '     i.service_id    = n.service_id     AND ' || CHR(10) ||
        '     i.subservice_id = n.subservice_id ' ||
        '    ) ' || CHR(10) ||
        ' WHEN MATCHED THEN UPDATE ' || CHR(10) ||
        '    SET i.item_total = n.item_total, ' || CHR(10) ||
        '        i.last_modified = :l_Calc_Date ' || CHR(10) ||
        ' WHEN NOT MATCHED THEN ' || CHR(10) ||
        '    INSERT ' || CHR(10) ||
        '        (REP_PERIOD_ID, EXTERNAL_ID, CREATE_DATE, LAST_MODIFIED, ' || CHR(10) || 
        '         ITEM_ID, ITEM_TYPE, ITEM_STATUS, CHARGE_TYPE, ' || CHR(10) ||
        '         BILL_ID, ORDER_ID, ORDER_BODY_ID, SERVICE_ID, SUBSERVICE_ID, ' || CHR(10) ||
        '         ITEM_TOTAL, DATE_FROM, DATE_TO, TAX_INCL) ' || CHR(10) ||
        '    VALUES (:l_Rep_Period_Id, n.external_id, :l_Calc_Date, :l_Calc_Date, ' || CHR(10) ||
        '            PK02_POID.Next_Item_Id, n.item_type, n.item_status, n.charge_type, ' || CHR(10) ||
        '            n.bill_id, n.order_id, n.order_body_id, n.service_id, n.subservice_id, ' || CHR(10) ||
        '            n.item_total, n.date_from, n.date_to, n.tax_incl)'   
    USING --l_External_Id, 
          pk00_const.c_ITEM_TYPE_BILL, pk00_const.c_ITEM_STATE_OPEN, 
          pk00_const.c_CHARGE_TYPE_MIN,
          l_Date_From, l_Date_To,
          l_R_Date_From, l_R_Date_To,
          l_Date_From, l_Date_To,
        --  l_BDR_Type,
          l_Rep_Period_Id,
          l_Calc_Date,
          l_Rep_Period_Id, l_Calc_Date, l_Calc_Date;       
    
    l_Merged := SQL%ROWCOUNT;
    
   -- ������� ������ ������ (���, � ������� ���� ���������� ������ ������� ���� �������, �.�. � ����.
   --  ������� ����������� ��� ���������� � ����������) 
   -- � ������ � ������, � ������� ������ ��� ���������, ���� ����� ����
    EXECUTE IMMEDIATE
        'DELETE item_t i ' || CHR(10) ||
        ' WHERE i.rep_period_id = :l_Rep_Period_Id ' || CHR(10) ||
        '   AND EXISTS (SELECT 1 ' || CHR(10) || -- ������� ������ �� �������� ������
        '                 FROM pin.bill_t b ' || CHR(10) ||
        '                WHERE b.bill_status IN (:l_Open) ' || CHR(10) ||
        '                  AND b.bill_id = i.bill_id) ' || CHR(10) ||
        '   AND (' ||
        '         i.external_id = :c_Network_NF ' || CHR(10) || -- = :l_External_Id
        '          OR ' || CHR(10) ||
        '         EXISTS (SELECT 1' || CHR(10) ||
        '                   FROM PIN.BDR_TYPES_T b ' || CHR(10) ||
        '                  WHERE b.oper = 1 ' || -- ������� ������� ��� ���������
        '                    AND b.network_id = i.external_id) ' || CHR(10) ||
        '       ) ' ||
        '   AND i.charge_type = :l_Charge_Type  ' || CHR(10) || 
        '   AND i.last_modified < :l_Calc_Date '       
    USING l_Rep_Period_Id, 
          PK00_CONST.c_BILL_STATE_OPEN, 
          c_Network_NF, --l_External_Id,
          pk00_const.c_CHARGE_TYPE_MIN,
          l_Calc_Date;
       
    l_Delete := SQL%ROWCOUNT;

    COMMIT;
       
    Pk01_Syslog.write_Msg(p_Msg => 'Calc. minim. pay ' ||
                                     'Period ' || TO_CHAR(p_Rep_Period, 'dd.mm.yyyy') ||
                                     ', Month: ' || TO_CHAR(l_Date_From, 'dd.mm.yyyy') ||
                                     ', Prep.: ' || TO_CHAR(l_Count) ||
                                     ': merged: ' || TO_CHAR(l_Merged) ||
                                     ', dlt: ' || TO_CHAR(l_Delete), 
                          p_Src => gc_PkgName || '.' || c_prcName);

END Load_Op_MinPay;


END pk114_Items;
/
