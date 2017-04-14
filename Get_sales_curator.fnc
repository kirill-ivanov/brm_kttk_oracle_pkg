CREATE OR REPLACE FUNCTION Get_sales_curator (
           p_branch_id     IN INTEGER,
           p_agent_id      IN INTEGER,
           p_contract_id   IN INTEGER,
           p_account_id    IN INTEGER,
           p_order_id      IN INTEGER,
           p_date          IN DATE
         ) RETURN VARCHAR2
IS
-- --------------------------------------------------------------------------------- --
-- получить координаты продавца
-- --------------------------------------------------------------------------------- --
    v_mgr VARCHAR2(300);
BEGIN
      SELECT TRIM(
             LAST_NAME||' '||
             SUBSTR(UPPER(FIRST_NAME),1,1)||DECODE(FIRST_NAME,NULL,'','.')||
             SUBSTR(UPPER(MIDDLE_NAME),1,1)||DECODE(MIDDLE_NAME,NULL,'','.')
             ) MGR_NAME
        INTO v_mgr
        FROM (
          SELECT M.LAST_NAME, M.FIRST_NAME, M.MIDDLE_NAME,
                 CASE 
                   WHEN SC.CONTRACTOR_ID = p_branch_id THEN 1
                   WHEN SC.CONTRACTOR_ID = p_agent_id  THEN 2
                   WHEN SC.CONTRACT_ID   IS NOT NULL   THEN 3
                   WHEN SC.ACCOUNT_ID    IS NOT NULL   THEN 4
                   WHEN SC.ORDER_ID      IS NOT NULL   THEN 5
                   ELSE 0
                 END  WT
            FROM SALE_CURATOR_T SC, MANAGER_T M
           WHERE M.MANAGER_ID = SC.MANAGER_ID
             AND NVL(p_date,SYSDATE) BETWEEN SC.DATE_FROM AND NVL(SC.DATE_TO,SYSDATE) 
             AND (SC.CONTRACTOR_ID = p_branch_id   OR
                  SC.CONTRACTOR_ID = p_agent_id    OR
                  SC.CONTRACT_ID   = p_contract_id OR 
                  SC.ACCOUNT_ID    = p_account_id  OR 
                  SC.ORDER_ID      = p_order_id )
          ORDER BY WT DESC
      )
      WHERE ROWNUM = 1
    ;  
    RETURN v_mgr;
EXCEPTION 
  WHEN NO_DATA_FOUND THEN
    RETURN NULL;
END;
/
