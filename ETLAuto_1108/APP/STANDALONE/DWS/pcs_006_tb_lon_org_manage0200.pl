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
DELETE FROM dw_sdata.PCS_006_TB_LON_ORG_MANAGE WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_006_TB_LON_ORG_MANAGE SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_339 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_006_TB_LON_ORG_MANAGE WHERE 1=0;

--Step2:
INSERT  INTO T_339 (
  LOAN_ID,
  CUR_ORG_ID,
  HISTORY_ORG_ID,
  CUR_ACC_ORG_ID,
  OWN_USER_ID,
  HISTORY_ACC_ORG_ID,
  PROVINCE_NUM,
  DELFLAG,
  CREATE_TIME,
  UPDATE_TIME,
  TRUNC_NO,
  OWN_ORG_ID,
  start_dt,
  end_dt)
SELECT
  N.LOAN_ID,
  N.CUR_ORG_ID,
  N.HISTORY_ORG_ID,
  N.CUR_ACC_ORG_ID,
  N.OWN_USER_ID,
  N.HISTORY_ACC_ORG_ID,
  N.PROVINCE_NUM,
  N.DELFLAG,
  N.CREATE_TIME,
  N.UPDATE_TIME,
  N.TRUNC_NO,
  N.OWN_ORG_ID,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(LOAN_ID, '' ) AS LOAN_ID ,
  COALESCE(CUR_ORG_ID, '' ) AS CUR_ORG_ID ,
  COALESCE(HISTORY_ORG_ID, '' ) AS HISTORY_ORG_ID ,
  COALESCE(CUR_ACC_ORG_ID, '' ) AS CUR_ACC_ORG_ID ,
  COALESCE(OWN_USER_ID, '' ) AS OWN_USER_ID ,
  COALESCE(HISTORY_ACC_ORG_ID, '' ) AS HISTORY_ACC_ORG_ID ,
  COALESCE(PROVINCE_NUM, '' ) AS PROVINCE_NUM ,
  COALESCE(DELFLAG, '' ) AS DELFLAG ,
  COALESCE(CREATE_TIME,'4999-12-31 00:00:00' ) AS CREATE_TIME ,
  COALESCE(UPDATE_TIME,'4999-12-31 00:00:00' ) AS UPDATE_TIME ,
  COALESCE(TRUNC_NO, 0 ) AS TRUNC_NO ,
  COALESCE(OWN_ORG_ID, '' ) AS OWN_ORG_ID 
 FROM  dw_tdata.PCS_006_TB_LON_ORG_MANAGE_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  LOAN_ID ,
  CUR_ORG_ID ,
  HISTORY_ORG_ID ,
  CUR_ACC_ORG_ID ,
  OWN_USER_ID ,
  HISTORY_ACC_ORG_ID ,
  PROVINCE_NUM ,
  DELFLAG ,
  CREATE_TIME ,
  UPDATE_TIME ,
  TRUNC_NO ,
  OWN_ORG_ID 
 FROM dw_sdata.PCS_006_TB_LON_ORG_MANAGE 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.LOAN_ID = T.LOAN_ID
WHERE
(T.LOAN_ID IS NULL)
 OR N.CUR_ORG_ID<>T.CUR_ORG_ID
 OR N.HISTORY_ORG_ID<>T.HISTORY_ORG_ID
 OR N.CUR_ACC_ORG_ID<>T.CUR_ACC_ORG_ID
 OR N.OWN_USER_ID<>T.OWN_USER_ID
 OR N.HISTORY_ACC_ORG_ID<>T.HISTORY_ACC_ORG_ID
 OR N.PROVINCE_NUM<>T.PROVINCE_NUM
 OR N.DELFLAG<>T.DELFLAG
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.UPDATE_TIME<>T.UPDATE_TIME
 OR N.TRUNC_NO<>T.TRUNC_NO
 OR N.OWN_ORG_ID<>T.OWN_ORG_ID
;

--Step3:
UPDATE dw_sdata.PCS_006_TB_LON_ORG_MANAGE P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_339
WHERE P.End_Dt=DATE('2100-12-31')
AND P.LOAN_ID=T_339.LOAN_ID
;

--Step4:
INSERT  INTO dw_sdata.PCS_006_TB_LON_ORG_MANAGE SELECT * FROM T_339;

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
