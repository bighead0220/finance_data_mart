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
DELETE FROM dw_sdata.BBS_001_CUST_BAIL_MAIN_ACCOUNT WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.BBS_001_CUST_BAIL_MAIN_ACCOUNT SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_45 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.BBS_001_CUST_BAIL_MAIN_ACCOUNT WHERE 1=0;

--Step2:
INSERT  INTO T_45 (
  ID,
  MAIN_ACC_NO,
  MAIN_ACC_NAME,
  CUST_ID,
  CUST_NO,
  BANK_ACCOUNT_NAME,
  USE_FLAG,
  OPR_TYPE,
  AUDIT_STATUS,
  CREATE_BRANCH_ID,
  LAST_UPD_BRANCH_ID,
  CREATE_OPR_ID,
  CREATE_TIME,
  LAST_UPD_OPR_ID,
  LAST_UPD_TIME,
  MAIN_BRANCH_NO,
  IRATE_SUB,
  start_dt,
  end_dt)
SELECT
  N.ID,
  N.MAIN_ACC_NO,
  N.MAIN_ACC_NAME,
  N.CUST_ID,
  N.CUST_NO,
  N.BANK_ACCOUNT_NAME,
  N.USE_FLAG,
  N.OPR_TYPE,
  N.AUDIT_STATUS,
  N.CREATE_BRANCH_ID,
  N.LAST_UPD_BRANCH_ID,
  N.CREATE_OPR_ID,
  N.CREATE_TIME,
  N.LAST_UPD_OPR_ID,
  N.LAST_UPD_TIME,
  N.MAIN_BRANCH_NO,
  N.IRATE_SUB,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID, 0 ) AS ID ,
  COALESCE(MAIN_ACC_NO, '' ) AS MAIN_ACC_NO ,
  COALESCE(MAIN_ACC_NAME, '' ) AS MAIN_ACC_NAME ,
  COALESCE(CUST_ID, 0 ) AS CUST_ID ,
  COALESCE(CUST_NO, '' ) AS CUST_NO ,
  COALESCE(BANK_ACCOUNT_NAME, '' ) AS BANK_ACCOUNT_NAME ,
  COALESCE(USE_FLAG, '' ) AS USE_FLAG ,
  COALESCE(OPR_TYPE, '' ) AS OPR_TYPE ,
  COALESCE(AUDIT_STATUS, '' ) AS AUDIT_STATUS ,
  COALESCE(CREATE_BRANCH_ID, 0 ) AS CREATE_BRANCH_ID ,
  COALESCE(LAST_UPD_BRANCH_ID, 0 ) AS LAST_UPD_BRANCH_ID ,
  COALESCE(CREATE_OPR_ID, 0 ) AS CREATE_OPR_ID ,
  COALESCE(CREATE_TIME, '' ) AS CREATE_TIME ,
  COALESCE(LAST_UPD_OPR_ID, 0 ) AS LAST_UPD_OPR_ID ,
  COALESCE(LAST_UPD_TIME, '' ) AS LAST_UPD_TIME ,
  COALESCE(MAIN_BRANCH_NO, '' ) AS MAIN_BRANCH_NO ,
  COALESCE(IRATE_SUB, '' ) AS IRATE_SUB 
 FROM  dw_tdata.BBS_001_CUST_BAIL_MAIN_ACCOUNT_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID ,
  MAIN_ACC_NO ,
  MAIN_ACC_NAME ,
  CUST_ID ,
  CUST_NO ,
  BANK_ACCOUNT_NAME ,
  USE_FLAG ,
  OPR_TYPE ,
  AUDIT_STATUS ,
  CREATE_BRANCH_ID ,
  LAST_UPD_BRANCH_ID ,
  CREATE_OPR_ID ,
  CREATE_TIME ,
  LAST_UPD_OPR_ID ,
  LAST_UPD_TIME ,
  MAIN_BRANCH_NO ,
  IRATE_SUB 
 FROM dw_sdata.BBS_001_CUST_BAIL_MAIN_ACCOUNT 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID = T.ID
WHERE
(T.ID IS NULL)
 OR N.MAIN_ACC_NO<>T.MAIN_ACC_NO
 OR N.MAIN_ACC_NAME<>T.MAIN_ACC_NAME
 OR N.CUST_ID<>T.CUST_ID
 OR N.CUST_NO<>T.CUST_NO
 OR N.BANK_ACCOUNT_NAME<>T.BANK_ACCOUNT_NAME
 OR N.USE_FLAG<>T.USE_FLAG
 OR N.OPR_TYPE<>T.OPR_TYPE
 OR N.AUDIT_STATUS<>T.AUDIT_STATUS
 OR N.CREATE_BRANCH_ID<>T.CREATE_BRANCH_ID
 OR N.LAST_UPD_BRANCH_ID<>T.LAST_UPD_BRANCH_ID
 OR N.CREATE_OPR_ID<>T.CREATE_OPR_ID
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.LAST_UPD_OPR_ID<>T.LAST_UPD_OPR_ID
 OR N.LAST_UPD_TIME<>T.LAST_UPD_TIME
 OR N.MAIN_BRANCH_NO<>T.MAIN_BRANCH_NO
 OR N.IRATE_SUB<>T.IRATE_SUB
;

--Step3:
UPDATE dw_sdata.BBS_001_CUST_BAIL_MAIN_ACCOUNT P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_45
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID=T_45.ID
;

--Step4:
INSERT  INTO dw_sdata.BBS_001_CUST_BAIL_MAIN_ACCOUNT SELECT * FROM T_45;

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
