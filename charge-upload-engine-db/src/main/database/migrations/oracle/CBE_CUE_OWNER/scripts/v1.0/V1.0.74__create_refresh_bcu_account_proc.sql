CREATE OR REPLACE PACKAGE PKG_CHARGING_ENGINE AS
 PROCEDURE prc_refresh_bcu_account;
END PKG_CHARGING_ENGINE;
/

CREATE OR REPLACE PACKAGE BODY PKG_CHARGING_ENGINE
AS
   PROCEDURE prc_refresh_bcu_account IS
   BEGIN
     DBMS_MVIEW.REFRESH (list                  => 'VWM_BCU_ACCOUNT'
                         ,method                => 'c'
                         ,rollback_seg          => NULL
                         ,push_deferred_rpc     => TRUE
                         ,refresh_after_errors  => FALSE
                         ,purge_option          => 1
                         ,parallelism           => 0
                         ,heap_size             => 0
                         ,atomic_refresh        => FALSE
                         ,nested                => FALSE
                         ,out_of_place          => FALSE
                         );
   END prc_refresh_bcu_account;
END PKG_CHARGING_ENGINE;
/