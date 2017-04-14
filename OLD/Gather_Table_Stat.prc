CREATE OR REPLACE PROCEDURE Gather_Table_Stat(l_Tab_Name varchar2)
IS
    PRAGMA autonomous_transaction;
BEGIN 

    dbms_stats.gather_table_stats(ownname => 'PIN',
                                  tabname => l_Tab_Name,
                                  no_invalidate => FALSE);

END;
/
