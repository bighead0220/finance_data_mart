#!/usr/bin/perl

use strict;     # Declare using Perl strict syntax
use DBI;        # If you are using other package, declare here

# ------------ Variable Section ------------
my ${AUTO_HOME} = $ENV{"AUTO_HOME"};

my ${WML_DB} = $ENV{"AUTO_WML_DB"};
if ( !defined(${WML_DB}) ) {
    ${WML_DB} = "WML";
}
my ${WTL_DB} = $ENV{"AUTO_WTL_DB"};
if ( !defined(${WTL_DB}) ) {
    ${WTL_DB} = "WTL";
}
my ${WMLVIEW_DB} = $ENV{"AUTO_WMLVIEW_DB"};
if ( !defined(${WMLVIEW_DB}) ) {
    ${WMLVIEW_DB} = "WMLVIEW";
}
my ${WTLVIEW_DB} = $ENV{"AUTO_WTLVIEW_DB"};
if ( !defined(${WTLVIEW_DB}) ) {
    ${WTLVIEW_DB} = "WTLVIEW";
}

my ${NULL_DATE} = "1900-01-02";
my ${MIN_DATE} = "1900-01-01";
my ${MAX_DATE} = "2100-12-31";

my ${LOGON_FILE} = "${AUTO_HOME}/etc/VERTICA_LOGON";
my ${LOGON_STR};
my ${CONTROL_FILE};
my ${TX_DATE};
my ${TX_DATE_YYYYMMDD};
my ${TX_MON_DAY_MMDD};

# ------------ VSQL function ------------
sub run_vsql_command
{
  #my $rc = open(VSQL, "${LOGON_STR}");
  my $rc = open(VSQL, "|vsql -h 22.224.65.171 -p 5433 -d CPCIMDB_TEST -U dwtrans -w dwtrans2016");

  unless ($rc) {
      print "Could not invoke VSQL command
";
      return -1;
  }

# ------ Below are VSQL scripts ----------
  print VSQL <<ENDOFINPUT;

\\set ON_ERROR_STOP on

--Step0:
DELETE FROM dw_sdata.BBS_001_CUST_BAIL_SUB_ACCOUNT WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.BBS_001_CUST_BAIL_SUB_ACCOUNT SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_46 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.BBS_001_CUST_BAIL_SUB_ACCOUNT WHERE 1=0;

--Step2:
INSERT  INTO T_46 (
  ID,
  MAIN_ACC_ID,
  SUB_ACC_NO,
  SUB_ACC_NAME,
  ACCEPT_CONTRACT_NO,
  TXN_TYPE,
  BAL_AMT,
  USE_BAL_AMT,
  SUB_ACC_TYPE,
  USE_FLAG,
  OPR_TYPE,
  AUDIT_STATUS,
  BRANCH_ID,
  BRANCH_TIME,
  CREATE_BRANCH_ID,
  LAST_UPD_BRANCH_ID,
  CREATE_OPR_ID,
  CREATE_TIME,
  LAST_UPD_OPR_ID,
  LAST_UPD_TIME,
  TXN_AMT,
  COST_AMT,
  BAIL_USE_FLAG,
  DUE_DATE,
  PAYER_ACC_NO,
  PAYER_NAME,
  OPERAT_TYPE,
  BAIL_TYPE,
  start_dt,
  end_dt)
SELECT
  N.ID,
  N.MAIN_ACC_ID,
  N.SUB_ACC_NO,
  N.SUB_ACC_NAME,
  N.ACCEPT_CONTRACT_NO,
  N.TXN_TYPE,
  N.BAL_AMT,
  N.USE_BAL_AMT,
  N.SUB_ACC_TYPE,
  N.USE_FLAG,
  N.OPR_TYPE,
  N.AUDIT_STATUS,
  N.BRANCH_ID,
  N.BRANCH_TIME,
  N.CREATE_BRANCH_ID,
  N.LAST_UPD_BRANCH_ID,
  N.CREATE_OPR_ID,
  N.CREATE_TIME,
  N.LAST_UPD_OPR_ID,
  N.LAST_UPD_TIME,
  N.TXN_AMT,
  N.COST_AMT,
  N.BAIL_USE_FLAG,
  N.DUE_DATE,
  N.PAYER_ACC_NO,
  N.PAYER_NAME,
  N.OPERAT_TYPE,
  N.BAIL_TYPE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID, 0 ) AS ID ,
  COALESCE(MAIN_ACC_ID, 0 ) AS MAIN_ACC_ID ,
  COALESCE(SUB_ACC_NO, '' ) AS SUB_ACC_NO ,
  COALESCE(SUB_ACC_NAME, '' ) AS SUB_ACC_NAME ,
  COALESCE(ACCEPT_CONTRACT_NO, '' ) AS ACCEPT_CONTRACT_NO ,
  COALESCE(TXN_TYPE, '' ) AS TXN_TYPE ,
  COALESCE(BAL_AMT, 0 ) AS BAL_AMT ,
  COALESCE(USE_BAL_AMT, 0 ) AS USE_BAL_AMT ,
  COALESCE(SUB_ACC_TYPE, '' ) AS SUB_ACC_TYPE ,
  COALESCE(USE_FLAG, '' ) AS USE_FLAG ,
  COALESCE(OPR_TYPE, '' ) AS OPR_TYPE ,
  COALESCE(AUDIT_STATUS, '' ) AS AUDIT_STATUS ,
  COALESCE(BRANCH_ID, 0 ) AS BRANCH_ID ,
  COALESCE(BRANCH_TIME, '' ) AS BRANCH_TIME ,
  COALESCE(CREATE_BRANCH_ID, 0 ) AS CREATE_BRANCH_ID ,
  COALESCE(LAST_UPD_BRANCH_ID, 0 ) AS LAST_UPD_BRANCH_ID ,
  COALESCE(CREATE_OPR_ID, 0 ) AS CREATE_OPR_ID ,
  COALESCE(CREATE_TIME, '' ) AS CREATE_TIME ,
  COALESCE(LAST_UPD_OPR_ID, 0 ) AS LAST_UPD_OPR_ID ,
  COALESCE(LAST_UPD_TIME, '' ) AS LAST_UPD_TIME ,
  COALESCE(TXN_AMT, 0 ) AS TXN_AMT ,
  COALESCE(COST_AMT, 0 ) AS COST_AMT ,
  COALESCE(BAIL_USE_FLAG, '' ) AS BAIL_USE_FLAG ,
  COALESCE(DUE_DATE, '' ) AS DUE_DATE ,
  COALESCE(PAYER_ACC_NO, '' ) AS PAYER_ACC_NO ,
  COALESCE(PAYER_NAME, '' ) AS PAYER_NAME ,
  COALESCE(OPERAT_TYPE, '' ) AS OPERAT_TYPE ,
  COALESCE(BAIL_TYPE, '' ) AS BAIL_TYPE 
 FROM  dw_tdata.BBS_001_CUST_BAIL_SUB_ACCOUNT_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID ,
  MAIN_ACC_ID ,
  SUB_ACC_NO ,
  SUB_ACC_NAME ,
  ACCEPT_CONTRACT_NO ,
  TXN_TYPE ,
  BAL_AMT ,
  USE_BAL_AMT ,
  SUB_ACC_TYPE ,
  USE_FLAG ,
  OPR_TYPE ,
  AUDIT_STATUS ,
  BRANCH_ID ,
  BRANCH_TIME ,
  CREATE_BRANCH_ID ,
  LAST_UPD_BRANCH_ID ,
  CREATE_OPR_ID ,
  CREATE_TIME ,
  LAST_UPD_OPR_ID ,
  LAST_UPD_TIME ,
  TXN_AMT ,
  COST_AMT ,
  BAIL_USE_FLAG ,
  DUE_DATE ,
  PAYER_ACC_NO ,
  PAYER_NAME ,
  OPERAT_TYPE ,
  BAIL_TYPE 
 FROM dw_sdata.BBS_001_CUST_BAIL_SUB_ACCOUNT 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID = T.ID
WHERE
(T.ID IS NULL)
 OR N.MAIN_ACC_ID<>T.MAIN_ACC_ID
 OR N.SUB_ACC_NO<>T.SUB_ACC_NO
 OR N.SUB_ACC_NAME<>T.SUB_ACC_NAME
 OR N.ACCEPT_CONTRACT_NO<>T.ACCEPT_CONTRACT_NO
 OR N.TXN_TYPE<>T.TXN_TYPE
 OR N.BAL_AMT<>T.BAL_AMT
 OR N.USE_BAL_AMT<>T.USE_BAL_AMT
 OR N.SUB_ACC_TYPE<>T.SUB_ACC_TYPE
 OR N.USE_FLAG<>T.USE_FLAG
 OR N.OPR_TYPE<>T.OPR_TYPE
 OR N.AUDIT_STATUS<>T.AUDIT_STATUS
 OR N.BRANCH_ID<>T.BRANCH_ID
 OR N.BRANCH_TIME<>T.BRANCH_TIME
 OR N.CREATE_BRANCH_ID<>T.CREATE_BRANCH_ID
 OR N.LAST_UPD_BRANCH_ID<>T.LAST_UPD_BRANCH_ID
 OR N.CREATE_OPR_ID<>T.CREATE_OPR_ID
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.LAST_UPD_OPR_ID<>T.LAST_UPD_OPR_ID
 OR N.LAST_UPD_TIME<>T.LAST_UPD_TIME
 OR N.TXN_AMT<>T.TXN_AMT
 OR N.COST_AMT<>T.COST_AMT
 OR N.BAIL_USE_FLAG<>T.BAIL_USE_FLAG
 OR N.DUE_DATE<>T.DUE_DATE
 OR N.PAYER_ACC_NO<>T.PAYER_ACC_NO
 OR N.PAYER_NAME<>T.PAYER_NAME
 OR N.OPERAT_TYPE<>T.OPERAT_TYPE
 OR N.BAIL_TYPE<>T.BAIL_TYPE
;

--Step3:
UPDATE dw_sdata.BBS_001_CUST_BAIL_SUB_ACCOUNT P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_46
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID=T_46.ID
;

--Step4:
INSERT  INTO dw_sdata.BBS_001_CUST_BAIL_SUB_ACCOUNT SELECT * FROM T_46;

COMMIT;

ENDOFINPUT

  close(VSQL);

  my $RET_CODE = $? >> 8;

  if ( $RET_CODE == 0 ) {
      return 0;
  }
  else {
      return 1;
  }
}

# ------------ main function ------------
sub main
{
   my $ret;
   open(LOGONFILE_H, "${LOGON_FILE}");
   ${LOGON_STR} = <LOGONFILE_H>;
   close(LOGONFILE_H);
   
   # Get the decoded logon string
   my($user,$passwd) = split(',',${LOGON_STR}); 
   #my $decodepasswd = `${AUTO_HOME}/bin/IceCode.exe -d "$passwd" "$user"`;                     
   #${LOGON_STR} = "|vsql -h 192.168.2.44 -p 5433 -d CPCIMDB_TEST -U ".$user." -w ".$decodepasswd;

   # Call vsql command to load data
   $ret = run_vsql_command();

   print "run_vsql_command() = $ret";
   return $ret;
}

# ------------ program section ------------
if ( $#ARGV < 0 ) {
   print "Usage: [perl 程序名 Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
";
   print "
";
   exit(1);
}

# Get the first argument
${CONTROL_FILE} = $ARGV[0];

if (${CONTROL_FILE} =~/[0-9]{8}($|\.)/) {
   ${TX_DATE_YYYYMMDD} = substr($&,0,8);
}
else{
   print "Usage: [perl 程序名 Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
";
   print "
";
   exit(1);
}

${TX_MON_DAY_MMDD} = substr(${TX_DATE_YYYYMMDD}, length(${TX_DATE_YYYYMMDD})-4,4);
${TX_DATE} = substr(${TX_DATE_YYYYMMDD}, 0, 4)."-".substr(${TX_DATE_YYYYMMDD}, 4, 2)."-".substr(${TX_DATE_YYYYMMDD}, 6, 2);
open(STDERR, ">&STDOUT");

my $ret = main();

exit($ret);
