CREATE OR REPLACE PACKAGE PK1110_OPR_RETRF_GUI
IS

    gc_PkgName CONSTANT varchar2(32) := 'PK1110_OPR_RETRF_GUI';

    /* ������� ��� �������� ������� �� ������������. ���������� id ���������� ������� ��� -1 ��� ������. 
       ������� ��������:
          p_Data_Type - �������� ��: 
                             SELECT bdr_code
                               FROM BDR_TYPES_T b
                              WHERE b.oper = 1
          p_ReCalc_Month - �����, ������ � ������� �������� ��������� (����� ��������� ������)                    
          p_Dest_Period  - �������� ������, � ������� �������� ������. ���� �� ����� �� ������� ������� �����.
          p_Not_Bill    - 1 - ������������� ������ ������, �� �������� � �����.
                          0 (�� ���������) ��� ����� ������ �������� - ��������������� ��� ����������
          p_Trf_Type    - ��� ����������, ���������� ���������
                            D - �����, R - ������, NULL - ������������� �� 
                            ��� ������� ����� ������� ���� ����� �� ����� X07 (�������������, ����. �� ��, ���������� � �.�.)                
          p_Date_From   - ������ ������� �������. ��� ��������� ������� �� ������� ��������� ������.
                          ���� �������� �����, �� p_ReCalc_Month ������������ 
          p_Date_To     - ����� ������� �������. ��� ��������� ������� �� ������� ��������� ������. ������������ ������
                          ��� �������� p_Date_From. ���� �������� �� �����, �� ������� ��������� ���� ������ 
                          ���� p_Date_From                      
          p_Msg         - OUT - ��������� (OK - ��� ��������� ��� ���������� �� ������)                
                          
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
                       
    /* ������� ��� �������� ����� ������� �� ������������ - �.�. ����� ����� � �� ����� ���� ������.
       ���������� id ��������� ������ ����������� ��� -1 � ������ ������ (p_Msg - ����������� ������)
       p_Task_Id       - id ��������� �������
       p_Order_Id      - id ������ (order_id), �� ������� ������ ���� ������
       p_Dest_Bill_Id  - id ����� (bill_id), � ������� ������ ������� ������� ������������������� ������.
                         ���� �� ������, �� ������ ����� � ������� (������, ��������� � ��������) �������� ����                                            
    */                           
    FUNCTION Create_Job_Detail(p_Task_Id      IN number,
                               p_Order_Id     IN number,
                               p_Dest_Bill_Id IN number,
                               p_Msg          OUT varchar2
                              ) RETURN number;

    -- ���������, ����������� �������� �������
    PROCEDURE Run_Job(p_Task_Id IN NUMBER);

    -- ������ ������� �� �������� �������
    FUNCTION Submit_Job(
                     p_otxt    OUT VARCHAR2,    -- ����� ���������� ��� �������� (�������� ������ � �.�.)
                     p_task_id IN NUMBER
                ) RETURN INTEGER; -- ��� ��������: c_RET_OK/c_RET_ERR

END PK1110_OPR_RETRF_GUI;
/
CREATE OR REPLACE PACKAGE BODY PK1110_OPR_RETRF_GUI
IS

    gc_DATE_END CONSTANT date := TO_DATE('01.01.2050','dd.mm.yyyy');


/* ������� ��� �������� ������� �� ������������. ���������� id ���������� ������� ��� -1 ��� ������. 
   ������� ��������:
      p_Data_Type - �������� ��: 
                         SELECT bdr_code
                           FROM BDR_TYPES_T b
                          WHERE b.oper = 1
      p_ReCalc_Month - �����, ������ � ������� �������� ��������� (����� ��������� ������)                    
      p_Dest_Period  - �������� ������, � ������� �������� ������. ���� �� ����� �� ������� ������� �����.
      p_Not_Bill    - 1 - ������������� ������ ������, �� �������� � �����.
                      0 (�� ���������) ��� ����� ������ �������� - ��������������� ��� ����������
      p_Trf_Type    - ��� ����������, ���������� ���������
                        D - �����, R - ������, NULL - ������������� �� 
                        ��� ������� ����� ������� ���� ����� �� ����� X07 (�������������, ����. �� ��, ���������� � �.�.)                
      p_Date_From   - ������ ������� �������. ��� ��������� ������� �� ������� ��������� ������.
                      ���� �������� �����, �� p_ReCalc_Month ������������ 
      p_Date_To     - ����� ������� �������. ��� ��������� ������� �� ������� ��������� ������. ������������ ������
                      ��� �������� p_Date_From. ���� �������� �� �����, �� ������� ��������� ���� ������ 
                      ���� p_Date_From                      
      p_Msg         - OUT - ��������� (OK - ��� ��������� ��� ���������� �� ������)                
                      
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

   -- ���������� id ���� ���������
    p_Msg := '��� ��������� ����� �����������.'; -- ������ ���� �� no_data_found ������
   
    SELECT b.bdr_type_id
      INTO l_BDR_Type_Id
      FROM BDR_TYPES_T b
     WHERE b.oper = 1
       AND b.bdr_code = p_Data_Type;

   -- ���������� ��� �������, ������� ���� �������������
    IF p_Trf_Type IN ('D','R') THEN
    
        l_Trf_Type := pk114_items.Get_List_BDR_Types(p_Data_Type => p_Data_Type,
                                                     p_Side      => NULL,
                                                     p_In_Out    => p_Trf_Type -- D - �����, R - ������
                                                    ); 
        
    ELSIF p_Trf_Type IS NULL THEN
        
        l_Trf_Type := pk114_items.Get_List_BDR_Types(p_Data_Type, NULL, 'D')  -- �����, ������
                    || ',' || pk114_items.Get_List_BDR_Types(p_Data_Type, NULL, 'R');  -- ������
    ELSE
    
        l_Trf_Type := p_Trf_Type;
       
    END IF;                                                     

   -- ���������� ������, �� ������� ������������� ������
    IF p_Date_From IS NULL THEN
    
        l_Date_From := TRUNC(p_ReCalc_Month,'mm');
        l_Date_To   := LAST_DAY(l_Date_From) + INTERVAL '0 23:59:59' DAY TO SECOND;
        
    ELSE
    
        -- �������� ������������ ���������� �������
        IF p_Date_From < NVL(p_Date_To, gc_DATE_END) THEN
            RAISE PERIOD_NOT_CORRECT;
        END IF;
            
        l_Date_From := p_Date_From;
        l_Date_To   := NVL(p_Date_To, LAST_DAY(TRUNC(l_Date_From)) + INTERVAL '0 23:59:59' DAY TO SECOND);
        
    END IF;    


    l_Dest_Period := NVL(p_Dest_Period, SYSDATE);
    
    -- �������� id ��������� ������� (������ �� ���� ������ ����� ��������)
    p_Msg := '�������� ������ �� ����� ��� ������.'; -- ������ ���� �� no_data_found ������
    
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
            NULL, NULL, '������������ �������')
    RETURN task_id INTO l_Result;

    COMMIT;

    p_Msg := 'OK';

    RETURN l_Result;

EXCEPTION
    WHEN no_data_found THEN
        RETURN -1;
        
    WHEN PERIOD_NOT_CORRECT THEN
        p_Msg := '������ ������� ����� �����������.'; 
        RETURN -1;     

END Create_Job;
             
/* ������� ��� �������� ����� ������� �� ������������ - �.�. ����� ����� � �� ����� ���� ������.
   ���������� id ��������� ������ ����������� ��� -1 � ������ ������ (p_Msg - ����������� ������)
   p_Task_Id       - id ��������� �������
   p_Order_Id      - id ������ (order_id), �� ������� ������ ���� ������
   p_Dest_Bill_Id  - id ����� (bill_id), � ������� ������ ������� ������� ������������������� ������.
                     ���� �� ������, �� ������ ����� � ������� (������, ��������� � ��������) �������� ����                                            
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
       -- ���� ����� ����, �� ��������� ��� ������� � ������������
       ---
         
        -- ���� id �������
        p_Msg := '������ ������ ��������� �������.'; -- �������� ������ ���� no_data_found
       
        SELECT rep_period
          INTO l_Rep_Period
          FROM q00_retrf_job
         WHERE task_id = p_Task_Id; 

        -- ��������� ���� � ��� ������ (������/������)
        p_Msg := '��������� ���� �� ����������, �� ����������� ��������� ������ '
                 || '��� ��������� � �������� �������, �������� �� ��������� � �������.'; -- �������� ������ ���� no_data_found 
        
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
       -- �������� �������
       
        
        IF pk00_const.Get_Period_Id(l_Rep_Period) != l_Result -- � �������� �/������ �� ��������� � �������� �����
        THEN   
           -- ������
            p_Msg := '�������� ������� ����� � ������� �� ���������.';
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
        p_Msg := '�������� ���� ������.';
        RETURN -1;

END Create_Job_Detail;


-- ==================================================================================================
-- ���������, ����������� �������� �������
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
    -- ��������� �����
    pk01_syslog.Write_Msg(p_Msg => 'Start. Task_id='||p_Task_Id, 
                          p_Src => gc_PkgName || '.' || lc_Prc_Name);
                          
    --
    -- ��������� � ������� ���� ������ ��������
    UPDATE Q00_RETRF_JOB
       SET start_time = SYSDATE
     WHERE task_id = p_Task_Id
    RETURNING TASK_ID, DATE_FROM, DATE_TO, DATA_TYPE, CREATE_DATE, 
              START_TIME, END_TIME, NOT_BILL, STATUS, REP_PERIOD, ELAPS_PERC, NOTE, OPR_TRF_TYPE   
         INTO lr_Task; -- ������ ����� �������� ��� ������ ����
     
    IF SQL%ROWCOUNT = 0 THEN
       RAISE NO_TASK_FOUND;
    END IF;
    
    COMMIT; -- ��������� update
    
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
    
    -- ��������� � ������� ���� ��������� �������� � ����� ������
    UPDATE Q00_RETRF_JOB
       SET end_time = SYSDATE,
           status   = '���������'
     WHERE task_id = p_Task_Id;
    
    -- ��������� �����
    pk01_syslog.Write_Msg(p_Msg => 'Stop. Task_id='||p_Task_Id, p_Src => gc_PkgName || '.' || lc_Prc_Name);
    --    
    COMMIT;
    
EXCEPTION    
    WHEN NO_TASK_FOUND THEN
        pk01_syslog.Write_Msg(p_Msg => '������� = '||p_Task_Id ||
                                       ' �� �������.', 
                          p_Src => gc_PkgName || '.' || lc_Prc_Name,
                          p_Level => pk01_syslog.l_Warn);         
    WHEN OTHERS THEN
        pk01_syslog.Insert_Error(p_Src => gc_PkgName || '.' || lc_Prc_Name);
END Run_Job;


-- ������ ������� �� �������� �������
FUNCTION Submit_Job(
                 p_otxt    OUT VARCHAR2,    -- ����� ���������� ��� �������� (�������� ������ � �.�.)
                 p_task_id IN NUMBER
            ) RETURN INTEGER -- ��� ��������: c_RET_OK/c_RET_ERR
IS
    lc_Prc_Name CONSTANT VARCHAR2(32) := 'Submit_Job';
    l_Job       BINARY_INTEGER;
    l_Task_Type NUMBER;
BEGIN
    -- 
    pk01_syslog.Write_Msg(p_Msg => 'Start. Task_id='||p_Task_Id, p_Src => gc_PkgName || '.' || lc_Prc_Name);
    --
    DBMS_JOB.SUBMIT(l_Job, 'PIN.Pk111_ReTrf_GUI.Run_Job(' || p_Task_Id || ');');
        
    COMMIT; -- ��������� Job
    
    p_otxt := '��'|| ' id='||p_task_id; 
    
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
