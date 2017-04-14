CREATE OR REPLACE PACKAGE PK10_PAYMENT_STAT
IS
    --
    -- Пакет для поддержки импорта данных из НБ
    -- event_t
    --
    -- ==============================================================================
    c_PkgName   CONSTANT varchar2(30) := 'PK10_PAYMENT_STAT';
    -- ==============================================================================
    c_RET_OK    CONSTANT integer := 0;
    c_RET_ER    CONSTANT integer :=-1;
    
    TYPE t_refc IS REF CURSOR;

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Отчет по платежам BRM по отчетным периодам
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Period_stat ( 
              p_message         OUT VARCHAR2, 
              p_recordset       OUT t_refc,
              p_period_id_from  INTEGER DEFAULT 201701
      );

    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    -- Отчет по платежам BRM начиная с указанного периода по дням
    -- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
    PROCEDURE Daily_stat ( 
              p_message         OUT VARCHAR2, 
              p_recordset       OUT t_refc,
              p_period_id_from  INTEGER DEFAULT 201701
      );

END PK10_PAYMENT_STAT;
/
CREATE OR REPLACE PACKAGE BODY PK10_PAYMENT_STAT
IS

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Отчет по платежам BRM по отчетным периодам
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Period_stat ( 
          p_message         OUT VARCHAR2, 
          p_recordset       OUT t_refc,
          p_period_id_from  INTEGER DEFAULT 201701
  )
IS
    v_prcName   CONSTANT varchar2(30) := 'Period_stat';
    v_calc_date DATE;
BEGIN
    -- дата начала периода
    v_calc_date := Pk04_Period.Period_from(p_period_id_from);
    
    OPEN p_recordset FOR 
      WITH GP AS (
          SELECT A.BILLING_ID, A.ACCOUNT_TYPE, A.ACCOUNT_ID,
                 BP.CALC_DATE, 
                 F.REGISTRY_DATE, 
                 BB.BANK_NAME,
                 --BP.PAYER_BANK, BP.BILL_PAYMENT_ID, BP.ACCOUNT_ID, BP.AMOUNT,
                 P.RECVD, P.TRANSFERED, P.CREATED_BY, P.REP_PERIOD_ID,
                 DECODE(A.ACCOUNT_ID, NULL, 1, 2, 1, 4, 1, 5, 1, 0) NOT_BIND,
                 DECODE(A.ACCOUNT_ID, NULL, 0, 2, 0, 4, 0, 5, 0, 1) BIND_OK,
                 CASE
                   WHEN A.ACCOUNT_ID NOT IN (2,4,5) AND P.TRANSFERED <> 0 AND P.TRANSFERED IS NOT NULL THEN 1
                   ELSE 0 
                 END TRANSFERED_OK,
                 CASE
                   WHEN A.ACCOUNT_ID NOT IN (2,4,5) AND (P.TRANSFERED = 0 OR P.TRANSFERED IS NULL) THEN 1
                   ELSE 0 
                 END NOT_TRANSFERED 
            FROM PAYMENT_GATE.BANK_PAYMENT_FILE F,
                 PAYMENT_GATE.BANK_PAYMENTS BP,
                 PAYMENT_GATE.BANKS BB,
                 ACCOUNT_T A, PAYMENT_T P
           WHERE BP.CALC_DATE >= v_calc_date
             AND BP.FILE_ID   = F.FILE_ID
             AND BP.BANK_ID   = BB.BANK_ID 
             AND BP.ACCOUNT_ID = A.ACCOUNT_ID(+)
             AND TO_CHAR(BP.BILL_PAYMENT_ID) = TO_CHAR(P.PAYMENT_ID(+))
      ), PERIOD_STAT AS (
          SELECT REP_PERIOD_ID,
                 COUNT(*) RECVD, 
                 SUM(DECODE(ACCOUNT_ID, NULL, 1, 0)) NOT_FOUND,
                 SUM(
                    CASE
                      WHEN ACCOUNT_ID IS NOT NULL AND ACCOUNT_ID NOT IN (4,5) THEN NOT_BIND
                      ELSE 0
                    END
                 ) NOT_BIND,
                 SUM(
                    CASE
                      WHEN ACCOUNT_ID = 4 THEN 1
                      ELSE 0
                    END
                 ) BIND_ACC4,
                 SUM(
                    CASE
                      WHEN ACCOUNT_ID = 5 THEN 1
                      ELSE 0
                    END
                 ) BIND_ACC5,
                 SUM(BIND_OK) BIND_OK,
                 SUM(TRANSFERED_OK) TRANSFERED_OK,
                 SUM(NOT_TRANSFERED) NOT_TRANSFERED,
                 TO_CHAR (
                     SUM(RECVD),
                        '999G999G999G999G999G999D99'
                 ) RECVD_AMOUNT,
                 TO_CHAR (
                     SUM(
                        CASE
                          WHEN ACCOUNT_ID = 4 THEN RECVD
                          ELSE 0
                        END
                     ), 
                    '999G999G999G999G999G999D99'
                 )
                 ACC4_AMOUNT,
                 TO_CHAR (
                     SUM(
                        CASE
                          WHEN ACCOUNT_ID = 5 THEN RECVD
                          ELSE 0
                        END
                     ), 
                    '999G999G999G999G999G999D99'
                 )
                 ACC5_AMOUNT,
                 TO_CHAR (
                     SUM(
                        CASE
                          WHEN BIND_OK = 1 THEN RECVD
                          ELSE 0
                        END
                     ), 
                    '999G999G999G999G999G999D99'
                 )
                 BIND_AMOUNT
            FROM GP
          GROUP BY REP_PERIOD_ID
          ORDER BY REP_PERIOD_ID NULLS LAST
      )
      SELECT * FROM PERIOD_STAT;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;

-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
-- Отчет по платежам BRM начиная с указанного периода по дням
-- - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - --
PROCEDURE Daily_stat ( 
          p_message         OUT VARCHAR2, 
          p_recordset       OUT t_refc,
          p_period_id_from  INTEGER DEFAULT 201701
  )
IS
    v_prcName   CONSTANT varchar2(30) := 'Daily_stat';
    v_calc_date DATE;
BEGIN
    -- дата начала периода
    v_calc_date := Pk04_Period.Period_from(p_period_id_from);
    
    OPEN p_recordset FOR 
      WITH GP AS (
          SELECT A.BILLING_ID, A.ACCOUNT_TYPE, A.ACCOUNT_ID,
                 BP.CALC_DATE, 
                 F.REGISTRY_DATE, 
                 BB.BANK_NAME,
                 --BP.PAYER_BANK, BP.BILL_PAYMENT_ID, BP.ACCOUNT_ID, BP.AMOUNT,
                 P.RECVD, P.TRANSFERED, P.CREATED_BY, P.REP_PERIOD_ID,
                 DECODE(A.ACCOUNT_ID, NULL, 1, 2, 1, 4, 1, 5, 1, 0) NOT_BIND,
                 DECODE(A.ACCOUNT_ID, NULL, 0, 2, 0, 4, 0, 5, 0, 1) BIND_OK,
                 CASE
                   WHEN A.ACCOUNT_ID NOT IN (2,4,5) AND P.TRANSFERED <> 0 AND P.TRANSFERED IS NOT NULL THEN 1
                   ELSE 0 
                 END TRANSFERED_OK,
                 CASE
                   WHEN A.ACCOUNT_ID NOT IN (2,4,5) AND (P.TRANSFERED = 0 OR P.TRANSFERED IS NULL) THEN 1
                   ELSE 0 
                 END NOT_TRANSFERED 
            FROM PAYMENT_GATE.BANK_PAYMENT_FILE F,
                 PAYMENT_GATE.BANK_PAYMENTS BP,
                 PAYMENT_GATE.BANKS BB,
                 ACCOUNT_T A, PAYMENT_T P
           WHERE BP.CALC_DATE >= v_calc_date
             AND BP.FILE_ID   = F.FILE_ID
             AND BP.BANK_ID   = BB.BANK_ID 
             AND BP.ACCOUNT_ID = A.ACCOUNT_ID(+)
             AND TO_CHAR(BP.BILL_PAYMENT_ID) = TO_CHAR(P.PAYMENT_ID(+))
      ), DAILY_STAT AS (
          SELECT REP_PERIOD_ID, REGISTRY_DATE,
                 COUNT(*) RECVD, 
                 SUM(DECODE(ACCOUNT_ID, NULL, 1, 0)) NOT_FOUND,
                 SUM(
                    CASE
                      WHEN ACCOUNT_ID IS NOT NULL AND ACCOUNT_ID NOT IN (4,5) THEN NOT_BIND
                      ELSE 0
                    END
                 ) NOT_BIND,
                 SUM(
                    CASE
                      WHEN ACCOUNT_ID = 4 THEN 1
                      ELSE 0
                    END
                 ) BIND_ACC4,
                 SUM(
                    CASE
                      WHEN ACCOUNT_ID = 5 THEN 1
                      ELSE 0
                    END
                 ) BIND_ACC5,
                 SUM(BIND_OK) BIND_OK,
                 SUM(TRANSFERED_OK) TRANSFERED_OK,
                 SUM(NOT_TRANSFERED) NOT_TRANSFERED,
                 TO_CHAR (
                     SUM(RECVD),
                        '999G999G999G999G999G999D99'
                 ) RECVD_AMOUNT,
                 TO_CHAR (
                     SUM(
                        CASE
                          WHEN ACCOUNT_ID = 4 THEN RECVD
                          ELSE 0
                        END
                     ), 
                    '999G999G999G999G999G999D99'
                 )
                 ACC4_AMOUNT,
                 TO_CHAR (
                     SUM(
                        CASE
                          WHEN ACCOUNT_ID = 5 THEN RECVD
                          ELSE 0
                        END
                     ), 
                    '999G999G999G999G999G999D99'
                 )
                 ACC5_AMOUNT,
                 TO_CHAR (
                     SUM(
                        CASE
                          WHEN BIND_OK = 1 THEN RECVD
                          ELSE 0
                        END
                     ), 
                    '999G999G999G999G999G999D99'
                 )
                 BIND_AMOUNT,
                 TO_CHAR(
                    SUM(DECODE(TRANSFERED, NULL, 0, TRANSFERED)),
                    '999G999G999G999G999G999D99'
                 ) 
                 TRANSFERED_AMOUNT
            FROM GP
          GROUP BY REP_PERIOD_ID, REGISTRY_DATE 
          ORDER BY REP_PERIOD_ID, REGISTRY_DATE NULLS LAST
      )
      SELECT * FROM DAILY_STAT;

EXCEPTION
    WHEN OTHERS THEN
        Pk01_SysLog.write_Error(NULL, c_PkgName||'.'||v_prcName);
        IF p_recordset%ISOPEN THEN 
            CLOSE p_recordset;
        END IF;
END;



END PK10_PAYMENT_STAT;
/
