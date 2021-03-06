CREATE OR REPLACE PACKAGE PK27_KTTK_AUDIT_2016
IS
    --
    -- C������ ������ �� ���������� �������� � �������� �������������
    --
    -- ==============================================================================
    c_PkgName   constant varchar2(30) := 'PK27_KTTK_AUDIT_2016';
    -- ==============================================================================
    c_RET_OK    constant integer := 0;
    c_RET_ER		constant integer :=-1;
    
    type t_refc is ref cursor;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 1. ��������� ������ ������
    --
    PROCEDURE Bills(p_message OUT VARCHAR2, p_recordset OUT t_refc );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 2. ��������� ������ ������� ������
    --
    PROCEDURE Items( p_message OUT VARCHAR2, p_recordset OUT t_refc );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- 3. �� ������������� �����������: 
    --    ������ �� item, ����� ������, ���� ��������. ������, �� ������� ��������� ������, �����, �����, ������.
    --
    -- ���������
    PROCEDURE Detail_rec_fixrates( p_message OUT VARCHAR2, p_recordset OUT t_refc );
               
    -- ������� �� ����������� ���������
    PROCEDURE Detail_min_fixrates( p_message OUT VARCHAR2, p_recordset OUT t_refc );

    -- -----------------------------------------------------------------------------------------------------
    -- 4. �� ��������� �������: ������ �� item, ����� ������, ���� ��������, �����������, ���-�� �������, �����, ����, �����, ������.
    --
    -- ���������� ������
    PROCEDURE Detail_clnt_voice(  p_message OUT VARCHAR2, p_recordset OUT t_refc );
    PROCEDURE Detail_clnt_voice_ctrl(  p_message OUT VARCHAR2, p_recordset OUT t_refc );

    -- ��������������� ������
    PROCEDURE Detail_oper_voice(  p_message OUT VARCHAR2, p_recordset OUT t_refc );
    PROCEDURE Detail_oper_voice_ctrl(  p_message OUT VARCHAR2, p_recordset OUT t_refc );

    -- -----------------------------------------------------------------------------------------------------
    -- 5. �� IP VPN:  ������ �� item, ����� ������, ���� ��������, qos, ����, �����, �����, �����, ������� �� ���������
    --
    PROCEDURE Detail_ip_vpn( p_message OUT VARCHAR2, p_recordset OUT t_refc );
    PROCEDURE Detail_ip_vpn_ctrl( p_message OUT VARCHAR2, p_recordset OUT t_refc );

    -- -----------------------------------------------------------------------------------------------------
    -- 6. �� IP Burst: ������ �� item, ����� ������, ���� ��������, ����������, �����, �����.
    --
    PROCEDURE Detail_ip_burst( p_message OUT VARCHAR2, p_recordset OUT t_refc );
    PROCEDURE Detail_ip_burst_ctrl( p_message OUT VARCHAR2, p_recordset OUT t_refc );

END PK27_KTTK_AUDIT_2016;
/
CREATE OR REPLACE PACKAGE BODY PK27_KTTK_AUDIT_2016
IS
-- ========================================================================= --
-- C������ ������ �� �������� ��� ������ (������ �. ��������� - ������)
-- ========================================================================= --
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 1. ��������� ������ ������
--
PROCEDURE Bills( 
               p_message      OUT VARCHAR2, 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Bills';
    v_retcode    INTEGER;
    v_count      INTEGER;
BEGIN
    -- ������� ������ ������ 
    DELETE FROM PK27_KTTK_BILL_T;
    -- ��������� ������� � ������� ������
    INSERT INTO PK27_KTTK_BILL_T
    SELECT
            B.BILL_ID,B.BILL_NO, B.BILL_DATE, B.BILL_TYPE, 
            B.REP_PERIOD_ID, B.TOTAL, B.GROSS, B.TAX, B.CURRENCY_ID, 
            C.CONTRACT_NO,C.DATE_FROM CONTRACT_DATE, C.DATE_TO CONTRACT_END_DATE, 
            A.ACCOUNT_NO, CM.COMPANY_NAME CLIENT, A.BILLING_ID, 
            CM.INN, AP.KPP
      FROM 
        BILL_T B, ACCOUNT_T A, ACCOUNT_PROFILE_T AP,
        CONTRACT_T C, COMPANY_T CM, 
        PK27_20160111_DRU_BILL_T T
     WHERE B.ACCOUNT_ID  = A.ACCOUNT_ID
       AND B.CONTRACT_ID = C.CONTRACT_ID
       AND B.PROFILE_ID  = AP.PROFILE_ID
       AND CM.ACTUAL(+)  = 'Y'
       AND CM.CONTRACT_ID(+)= C.CONTRACT_ID
       AND B.REP_PERIOD_ID BETWEEN 201601 and 201611
       AND A.ACCOUNT_TYPE = 'J'
       --AND A.BILLING_ID IN (2001,2002,2003,2009,2004,2005,2006)
       -- AND B.BILL_STATUS IN ('READY','CLOSED')
       -- AND (A.STATUS = 'B' OR B.BILL_STATUS <> 'B')
       -- AND A.STATUS <> 'T'
       -- AND B.TOTAL <> 0
       AND B.BILL_TYPE NOT IN ('I')
       AND B.REP_PERIOD_ID = T.REP_PERIOD_ID
       AND B.BILL_ID = T.BILL_ID
    ORDER BY B.REP_PERIOD_ID,B.BILL_NO;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK27_KTTK_BILL_T '||v_count||' rows - inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ������������ ���������
    COMMIT;
    -- ���������� ������
    OPEN p_recordset FOR    
        SELECT * FROM LL_BILL_T_20161227
         ORDER BY REP_PERIOD_ID, BILL_NO
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 2. ��������� ������ ������� ������
--
PROCEDURE Items( 
               p_message      OUT VARCHAR2, 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Items';
    v_retcode    INTEGER;
    v_count      INTEGER;
BEGIN
    -- ������� ������ ������ 
    DELETE FROM PK27_KTTK_ITEM_T;
    -- ��������� ������� � ������� ������
    INSERT INTO PK27_KTTK_ITEM_T
    SELECT
        B.BILL_ID,B.BILL_NO, B.BILL_TYPE, B.REP_PERIOD_ID, I.ITEM_ID, I.ITEM_TYPE, I.CHARGE_TYPE, 
        O.ORDER_ID, O.ORDER_NO, O.DATE_FROM ORDER_DATE_FROM, O.DATE_TO ORDER_DATE_TO,
        S.SERVICE_ID, S.SERVICE, 
        I.ORDER_BODY_ID, SS.SUBSERVICE_ID, SS.SUBSERVICE,
        I.REP_GROSS, I.REP_TAX,  I.BILL_TOTAL, I.TAX_INCL, I.ITEM_TOTAL, I.ITEM_CURRENCY_ID, I.ITEM_CURRENCY_RATE   
    FROM  PK27_KTTK_BILL_T B, ITEM_T I, ORDER_T O, SERVICE_T S, SUBSERVICE_T SS
    WHERE I.BILL_ID = B.BILL_ID    
      AND I.ORDER_ID = O.ORDER_ID
      AND S.SERVICE_ID = I.SERVICE_ID
      AND SS.SUBSERVICE_ID = I.SUBSERVICE_ID
    ORDER BY B.REP_PERIOD_ID, B.BILL_NO;
    v_count := SQL%ROWCOUNT;
    Pk01_Syslog.Write_msg('PK27_KTTK_ITEM_T '||v_count||' rows - inserted', c_PkgName||'.'||v_prcName, Pk01_Syslog.L_info );
    -- ������������ ���������
    COMMIT;
    -- ���������� ������
    OPEN p_recordset FOR    
        SELECT * FROM LL_ITEM_T_20161227
         ORDER BY REP_PERIOD_ID, BILL_NO
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- 3. �� ������������� �����������: 
--    ������ �� item, ����� ������, ���� ��������. ������, �� ������� ��������� ������, �����, �����, ������.
--
-- ���������
PROCEDURE Detail_rec_fixrates( 
               p_message      OUT VARCHAR2, 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Detail_rec_fixrates';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR    
        SELECT 
               LL.REP_PERIOD_ID, LL.BILL_ID, LL.BILL_NO,
               LL.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE,
               LL.SERVICE, LL.SUBSERVICE,
               LL.TAX_INCL, LL.ITEM_CURRENCY_ID, LL.ITEM_TOTAL,
               CASE
               WHEN OB.RATE_RULE_ID IN (2402,2403,2417) THEN TO_CHAR(OB.RATE_VALUE)
               ELSE '������������� ������ ���������' 
               END RATE_VALUE, 
               OB.DATE_FROM RATEPLAN_FROM, OB.DATE_TO RATEPLAN_TO, OB.RATE_RULE_ID,
               LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO
          FROM PK27_KTTK_ITEM_T LL, ORDER_BODY_T OB
         WHERE LL.CHARGE_TYPE = 'REC' 
           AND LL.ORDER_BODY_ID = OB.ORDER_BODY_ID
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ������� �� ����������� ���������
PROCEDURE Detail_min_fixrates( 
               p_message      OUT VARCHAR2, 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Detail_min_fixrates';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR    
        SELECT LL.REP_PERIOD_ID, LL.BILL_ID, LL.BILL_NO, 
               LL.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, 
               LL.TAX_INCL, LL.ITEM_CURRENCY_ID, LL.ITEM_TOTAL,
               NVL(TO_CHAR(OB.RATE_VALUE), '������������� ������') RATE_VALUE, 
               OB.DATE_FROM RATEPLAN_FROM, OB.DATE_TO RATEPLAN_TO,
               OB.RATE_RULE_ID,
               LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO
          FROM PK27_KTTK_ITEM_T LL, ORDER_BODY_T OB
         WHERE LL.CHARGE_TYPE = 'MIN' 
           AND LL.ORDER_BODY_ID = OB.ORDER_BODY_ID
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- -----------------------------------------------------------------------------------------------------
-- 4. �� ��������� �������: ������ �� item, ����� ������, ���� ��������, �����������, ���-�� �������, �����, ����, �����, ������.
--
-- ���������� ������
PROCEDURE Detail_clnt_voice( 
               p_message      OUT VARCHAR2, 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Detail_clnt_voice';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR    
        SELECT D.REP_PERIOD_ID, D.BILL_ID, LL.BILL_NO, LL.BILL_TYPE, 
               D.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, 
               LL.SERVICE, LL.SUBSERVICE,
               D.TERM_Z_NAME, 
               SUM(D.CALLS) CALLS, 
               SUM(D.MINUTES) MINUTES, 
               SUM(D.TOTAL) TOTAL, 
               D.TARIFF_AMOUNT, D.TARIFF_CURRENCY_ID, LL.TAX_INCL,
               LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO   
          FROM PK27_KTTK_ITEM_T LL, DETAIL_MMTS_T_JUR D
         WHERE LL.REP_PERIOD_ID = D.REP_PERIOD_ID
           AND LL.BILL_ID       = D.BILL_ID
           AND LL.ITEM_ID       = D.ITEM_ID
         GROUP BY D.REP_PERIOD_ID, D.BILL_ID, LL.BILL_NO, LL.BILL_TYPE, D.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE,  
               LL.SERVICE, LL.SUBSERVICE,
               D.TERM_Z_NAME, 
               TARIFF_AMOUNT, TARIFF_CURRENCY_ID, LL.TAX_INCL,
               LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO
         ORDER BY D.REP_PERIOD_ID, D.BILL_ID, D.ITEM_ID
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- �������� ������������ ������
PROCEDURE Detail_clnt_voice_ctrl( 
               p_message      OUT VARCHAR2, 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Detail_clnt_voice_ctrl';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR    
        WITH RP AS (
            SELECT D.REP_PERIOD_ID, D.BILL_ID, LL.BILL_NO, LL.BILL_TYPE, 
                   D.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, 
                   LL.SERVICE, LL.SUBSERVICE,
                   D.TERM_Z_NAME, 
                   SUM(D.CALLS) CALLS, 
                   SUM(D.MINUTES) MINUTES, 
                   SUM(D.TOTAL) TOTAL, 
                   D.TARIFF_AMOUNT, D.TARIFF_CURRENCY_ID, LL.TAX_INCL,
                   LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO   
              FROM PK27_KTTK_ITEM_T LL, DETAIL_MMTS_T_JUR D
             WHERE LL.REP_PERIOD_ID = D.REP_PERIOD_ID
               AND LL.BILL_ID       = D.BILL_ID
               AND LL.ITEM_ID       = D.ITEM_ID
             GROUP BY D.REP_PERIOD_ID, D.BILL_ID, LL.BILL_NO, LL.BILL_TYPE, D.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE,  
                   LL.SERVICE, LL.SUBSERVICE,
                   D.TERM_Z_NAME, 
                   TARIFF_AMOUNT, TARIFF_CURRENCY_ID, LL.TAX_INCL,
                   LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO
        )
        SELECT RP.BILL_NO, RP.BILL_TYPE, RP.BILL_ID, 
               RP.REP_PERIOD_ID, RP.ITEM_ID, I.ITEM_TYPE, 
               SUM(TOTAL) SUM_TOTAL, MIN(I.ITEM_TOTAL) ITEM_TOTAL 
          FROM RP, ITEM_T I
         WHERE RP.REP_PERIOD_ID = I.REP_PERIOD_ID 
           AND RP.ITEM_ID = I.ITEM_ID
         GROUP BY RP.BILL_NO, RP.BILL_TYPE, RP.BILL_ID, RP.REP_PERIOD_ID, RP.ITEM_ID, I.ITEM_TYPE
         HAVING ABS(ROUND(SUM(TOTAL),2) - ROUND(MIN(I.ITEM_TOTAL),2)) > 1
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - -  - - - - - - - - - - - - --
-- ��������������� ������
PROCEDURE Detail_oper_voice( 
               p_message      OUT VARCHAR2, 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Detail_oper_voice';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR    
        SELECT DB.PERIOD_ID REP_PERIOD_ID, DB.BILL_ID, LL.BILL_NO, LL.BILL_TYPE, 
               DB.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, LL.SERVICE, LL.SUBSERVICE,
               DB.ZONE,
               SUM(DB.CALLS_NUM) CALLS,
               SUM(DB.MINS) MINUTES,
               SUM(DB.GROSS) TOTAL,
               DB.TARIFF_MIN TARIFF_AMOUNT,
               DB.CURRENCY_ID TARIFF_CURRENCY_ID, LL.TAX_INCL,
               LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO
          FROM PK27_KTTK_ITEM_T LL, DETAIL_BSRV_T DB
         WHERE LL.REP_PERIOD_ID = DB.PERIOD_ID
           AND LL.BILL_ID       = DB.BILL_ID
           AND LL.ITEM_ID       = DB.ITEM_ID
         GROUP BY DB.PERIOD_ID, DB.BILL_ID, LL.BILL_NO, LL.BILL_TYPE,
               DB.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, LL.SERVICE, LL.SUBSERVICE,
               DB.ZONE,
               DB.TARIFF_MIN,
               DB.CURRENCY_ID, LL.TAX_INCL,
               LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO
         ORDER BY DB.PERIOD_ID, DB.BILL_ID, DB.ITEM_ID
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ��������
PROCEDURE Detail_oper_voice_ctrl( 
               p_message      OUT VARCHAR2, 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Detail_oper_voice_ctrl';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR    
        WITH RP AS (
            SELECT DB.PERIOD_ID REP_PERIOD_ID, DB.BILL_ID, LL.BILL_NO, LL.BILL_TYPE, 
                   DB.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, LL.SERVICE, LL.SUBSERVICE,
                   DB.ZONE,
                   SUM(DB.CALLS_NUM) CALLS,
                   SUM(DB.MINS) MINUTES,
                   SUM(DB.GROSS) TOTAL,
                   DB.TARIFF_MIN TARIFF_AMOUNT,
                   DB.CURRENCY_ID TARIFF_CURENCY_ID, LL.TAX_INCL,
                   LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO
              FROM PK27_KTTK_ITEM_T LL, DETAIL_BSRV_T DB
             WHERE LL.REP_PERIOD_ID = DB.PERIOD_ID
               AND LL.BILL_ID       = DB.BILL_ID
               AND LL.ITEM_ID       = DB.ITEM_ID
             GROUP BY DB.PERIOD_ID, DB.BILL_ID, LL.BILL_NO, LL.BILL_TYPE,
                   DB.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, LL.SERVICE, LL.SUBSERVICE,
                   DB.ZONE,
                   DB.TARIFF_MIN,
                   DB.CURRENCY_ID, LL.TAX_INCL,
                   LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO
        )
        SELECT RP.BILL_NO, RP.BILL_TYPE, RP.BILL_ID, 
               RP.REP_PERIOD_ID, RP.ITEM_ID, I.ITEM_TYPE, 
               SUM(TOTAL) SUM_TOTAL, MIN(I.ITEM_TOTAL) ITEM_TOTAL
          FROM RP, ITEM_T I
         WHERE RP.REP_PERIOD_ID = I.REP_PERIOD_ID 
           AND RP.ITEM_ID = I.ITEM_ID
         GROUP BY RP.BILL_NO, RP.BILL_TYPE, RP.BILL_ID, RP.REP_PERIOD_ID, RP.ITEM_ID, I.ITEM_TYPE
         HAVING ABS(ROUND(SUM(TOTAL),2) - ROUND(MIN(I.ITEM_TOTAL),2)) > 1
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;


-- -----------------------------------------------------------------------------------------------------
-- 5. �� IP VPN:  ������ �� item, ����� ������, ���� ��������, qos, ����, �����, �����, �����, ������� �� ���������
--
PROCEDURE Detail_ip_vpn( 
               p_message      OUT VARCHAR2, 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Detail_ip_vpn';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR    
        SELECT LL.REP_PERIOD_ID, LL.BILL_ID, LL.BILL_NO, LL.BILL_TYPE,
               LL.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, LL.SERVICE, LL.SUBSERVICE,
               SUM(CB.VOLUME) VOLUME, 
               DU.NAME VOLUME_UNIT,
               SUM(CB.AMOUNT) TOTAL,
               CB.PRICE TARIFF_AMOUNT,
               CB.CURRENCY_ID,
               CB.TAX_INCL,
               CB.ZONE_IN, CB.ZONE_OUT, 
               DQ.NAME QOS,
               LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO 
          FROM PK27_KTTK_ITEM_T LL, BDR_CCAD_T CB, DICTIONARY_T DU, DICTIONARY_T DQ
         WHERE LL.REP_PERIOD_ID = CB.REP_PERIOD_ID
           AND LL.BILL_ID       = CB.BILL_ID
           AND LL.ITEM_ID       = CB.ITEM_ID
           AND LL.CHARGE_TYPE   = 'USG'
           AND CB.RATE_RULE_ID NOT IN (2409, 2418)
           AND CB.VOLUME_UNIT_ID = DU.KEY_ID
           AND CB.QUALITY_ID    = DQ.KEY_ID(+)
         GROUP BY LL.REP_PERIOD_ID, LL.BILL_ID, LL.BILL_NO, LL.BILL_TYPE, 
               LL.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, LL.SERVICE, LL.SUBSERVICE,
               DU.NAME, CB.PRICE, CB.CURRENCY_ID, CB.TAX_INCL, CB.ZONE_IN, CB.ZONE_OUT, DQ.NAME,
               LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ��������
PROCEDURE Detail_ip_vpn_ctrl( 
               p_message      OUT VARCHAR2, 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Detail_ip_vpn_ctrl';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR    
        WITH RP AS (
            SELECT LL.REP_PERIOD_ID, LL.BILL_ID, LL.BILL_NO, LL.BILL_TYPE,
                   LL.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, LL.SERVICE, LL.SUBSERVICE,
                   SUM(CB.VOLUME) VOLUME, 
                   DU.NAME VOLUME_UNIT,
                   SUM(CB.AMOUNT) TOTAL,
                   CB.PRICE TARIFF_AMOUNT,
                   CB.CURRENCY_ID,
                   CB.TAX_INCL,
                   CB.ZONE_IN, CB.ZONE_OUT, 
                   DQ.NAME QOS,
                   LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO 
              FROM PK27_KTTK_ITEM_T LL, BDR_CCAD_T CB, DICTIONARY_T DU, DICTIONARY_T DQ
             WHERE LL.REP_PERIOD_ID = CB.REP_PERIOD_ID
               AND LL.BILL_ID       = CB.BILL_ID
               AND LL.ITEM_ID       = CB.ITEM_ID
               AND LL.CHARGE_TYPE   = 'USG'
               AND CB.RATE_RULE_ID NOT IN (2409, 2418)
               AND CB.VOLUME_UNIT_ID = DU.KEY_ID
               AND CB.QUALITY_ID    = DQ.KEY_ID(+)
             GROUP BY LL.REP_PERIOD_ID, LL.BILL_ID, LL.BILL_NO, LL.BILL_TYPE, 
                   LL.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, LL.SERVICE, LL.SUBSERVICE,
                   DU.NAME, CB.PRICE, CB.CURRENCY_ID, CB.TAX_INCL, CB.ZONE_IN, CB.ZONE_OUT, DQ.NAME,
                   LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO
        )
        SELECT RP.BILL_NO, RP.BILL_TYPE, RP.BILL_ID, RP.REP_PERIOD_ID, 
               RP.ITEM_ID, I.ITEM_TYPE, SUM(TOTAL) , MIN(I.ITEM_TOTAL) ITEM_TOTAL
          FROM RP, ITEM_T I
         WHERE RP.REP_PERIOD_ID = I.REP_PERIOD_ID 
           AND RP.ITEM_ID = I.ITEM_ID
         GROUP BY RP.BILL_NO, RP.BILL_TYPE, RP.BILL_ID, RP.REP_PERIOD_ID, RP.ITEM_ID, I.ITEM_TYPE
         HAVING ABS(ROUND(SUM(TOTAL),2) - ROUND(MIN(I.ITEM_TOTAL),2)) > 1
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- -----------------------------------------------------------------------------------------------------
-- 6. �� IP Burst: ������ �� item, ����� ������, ���� ��������, ����������, �����, �����.
--
PROCEDURE Detail_ip_burst( 
               p_message      OUT VARCHAR2, 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Detail_ip_burst';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR    
        SELECT LL.REP_PERIOD_ID, LL.BILL_ID, LL.BILL_NO, LL.BILL_TYPE, 
               LL.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, LL.SERVICE, LL.SUBSERVICE,
               SUM(CB.EXCESS_SPEED) EXCESS_SPEED,
               D.NAME EXCESS_SPEED_UNIT, 
               SUM(CB.AMOUNT) TOTAL,
               CB.PRICE TARIFF_AMOUNT,
               CB.CURRENCY_ID,
               CB.TAX_INCL,
               LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO
          FROM PK27_KTTK_ITEM_T LL, BDR_CCAD_T CB, DICTIONARY_T D
         WHERE LL.REP_PERIOD_ID = CB.REP_PERIOD_ID
           AND LL.BILL_ID       = CB.BILL_ID
           AND LL.ITEM_ID       = CB.ITEM_ID
           AND LL.CHARGE_TYPE   = 'USG'
           AND CB.RATE_RULE_ID IN (2409, 2418)
           AND CB.EXCESS_SPEED_UNIT = D.KEY_ID
         GROUP BY LL.REP_PERIOD_ID, LL.BILL_ID, LL.BILL_NO, LL.BILL_TYPE, 
               LL.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, LL.SERVICE, LL.SUBSERVICE,
               D.NAME,
               CB.PRICE,
               CB.CURRENCY_ID,
               CB.TAX_INCL,
               LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO
         ORDER BY LL.REP_PERIOD_ID, LL.BILL_ID, LL.ITEM_ID
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

-- ��������
PROCEDURE Detail_ip_burst_ctrl( 
               p_message      OUT VARCHAR2, 
               p_recordset    OUT t_refc
           )
IS
    v_prcName    CONSTANT VARCHAR2(30) := 'Detail_ip_burst_ctrl';
    v_retcode    INTEGER;
BEGIN
    -- ���������� ������
    OPEN p_recordset FOR    
        WITH RP AS (
            SELECT LL.REP_PERIOD_ID, LL.BILL_ID, LL.BILL_NO, LL.BILL_TYPE, 
                   LL.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, LL.SERVICE, LL.SUBSERVICE,
                   SUM(CB.EXCESS_SPEED) EXCESS_SPEED,
                   D.NAME EXCESS_SPEED_UNIT, 
                   SUM(CB.AMOUNT) TOTAL,
                   CB.PRICE TARIFF_AMOUNT,
                   CB.CURRENCY_ID,
                   CB.TAX_INCL,
                   LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO
              FROM PK27_KTTK_ITEM_T LL, BDR_CCAD_T CB, DICTIONARY_T D
             WHERE LL.REP_PERIOD_ID = CB.REP_PERIOD_ID
               AND LL.BILL_ID       = CB.BILL_ID
               AND LL.ITEM_ID       = CB.ITEM_ID
               AND LL.CHARGE_TYPE   = 'USG'
               AND CB.RATE_RULE_ID IN (2409, 2418)
               AND CB.EXCESS_SPEED_UNIT = D.KEY_ID
             GROUP BY LL.REP_PERIOD_ID, LL.BILL_ID, LL.BILL_NO, LL.BILL_TYPE, 
                   LL.ITEM_ID, LL.ITEM_TYPE, LL.CHARGE_TYPE, LL.SERVICE, LL.SUBSERVICE,
                   D.NAME,
                   CB.PRICE,
                   CB.CURRENCY_ID,
                   CB.TAX_INCL,
                   LL.ORDER_ID, LL.ORDER_NO, LL.ORDER_DATE_FROM, LL.ORDER_DATE_TO
             ORDER BY LL.REP_PERIOD_ID, LL.BILL_ID, LL.ITEM_ID
        )
        SELECT RP.BILL_NO, RP.BILL_TYPE, RP.BILL_ID, 
               RP.REP_PERIOD_ID, RP.ITEM_ID, I.ITEM_TYPE, 
               SUM(TOTAL) SUM_TOTAL, MIN(I.ITEM_TOTAL) ITEM_TOTAL 
          FROM RP, ITEM_T I
         WHERE RP.REP_PERIOD_ID = I.REP_PERIOD_ID 
           AND RP.ITEM_ID = I.ITEM_ID
         GROUP BY RP.BILL_NO, RP.BILL_TYPE, RP.BILL_ID, RP.REP_PERIOD_ID, RP.ITEM_ID, I.ITEM_TYPE
         HAVING ABS(ROUND(SUM(TOTAL),2) - ROUND(MIN(I.ITEM_TOTAL),2)) > 1
        ;
EXCEPTION
    WHEN OTHERS THEN
        v_retcode := Pk01_SysLog.Fn_write_Error('ERROR', c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
        RAISE_APPLICATION_ERROR(Pk01_SysLog.n_APP_EXCEPTION, 'msg_id='||v_retcode||':'||c_PkgName||'.'||v_prcName);
END;

END PK27_KTTK_AUDIT_2016;
/
