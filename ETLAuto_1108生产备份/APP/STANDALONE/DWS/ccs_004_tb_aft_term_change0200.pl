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
DELETE FROM dw_sdata.CCS_004_TB_AFT_TERM_CHANGE WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCS_004_TB_AFT_TERM_CHANGE SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_353 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCS_004_TB_AFT_TERM_CHANGE WHERE 1=0;

--Step2:
INSERT  INTO T_353 (
  TERM_CHANGE_ID,
  TERM_CHANGE_NUM,
  CONTRACT_NUM,
  CUSTOMER_NUM,
  FLOW_STATUS,
  HANDLING_USER_NUM,
  HANDLING_ORG_CD,
  HANDLING_DATE,
  FXBS_TIME_MARK,
  BIZ_PRODUCT_CD,
  CONCLUSION_CD,
  start_dt,
  end_dt)
SELECT
  N.TERM_CHANGE_ID,
  N.TERM_CHANGE_NUM,
  N.CONTRACT_NUM,
  N.CUSTOMER_NUM,
  N.FLOW_STATUS,
  N.HANDLING_USER_NUM,
  N.HANDLING_ORG_CD,
  N.HANDLING_DATE,
  N.FXBS_TIME_MARK,
  N.BIZ_PRODUCT_CD,
  N.CONCLUSION_CD,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(TERM_CHANGE_ID, '' ) AS TERM_CHANGE_ID ,
  COALESCE(TERM_CHANGE_NUM, '' ) AS TERM_CHANGE_NUM ,
  COALESCE(CONTRACT_NUM, '' ) AS CONTRACT_NUM ,
  COALESCE(CUSTOMER_NUM, '' ) AS CUSTOMER_NUM ,
  COALESCE(FLOW_STATUS, '' ) AS FLOW_STATUS ,
  COALESCE(HANDLING_USER_NUM, '' ) AS HANDLING_USER_NUM ,
  COALESCE(HANDLING_ORG_CD, '' ) AS HANDLING_ORG_CD ,
  COALESCE(HANDLING_DATE,'4999-12-31 00:00:00' ) AS HANDLING_DATE ,
  COALESCE(FXBS_TIME_MARK,'4999-12-31 00:00:00' ) AS FXBS_TIME_MARK ,
  COALESCE(BIZ_PRODUCT_CD, '' ) AS BIZ_PRODUCT_CD ,
  COALESCE(CONCLUSION_CD, '' ) AS CONCLUSION_CD 
 FROM  dw_tdata.CCS_004_TB_AFT_TERM_CHANGE_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  TERM_CHANGE_ID ,
  TERM_CHANGE_NUM ,
  CONTRACT_NUM ,
  CUSTOMER_NUM ,
  FLOW_STATUS ,
  HANDLING_USER_NUM ,
  HANDLING_ORG_CD ,
  HANDLING_DATE ,
  FXBS_TIME_MARK ,
  BIZ_PRODUCT_CD ,
  CONCLUSION_CD 
 FROM dw_sdata.CCS_004_TB_AFT_TERM_CHANGE 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.TERM_CHANGE_ID = T.TERM_CHANGE_ID
WHERE
(T.TERM_CHANGE_ID IS NULL)
 OR N.TERM_CHANGE_NUM<>T.TERM_CHANGE_NUM
 OR N.CONTRACT_NUM<>T.CONTRACT_NUM
 OR N.CUSTOMER_NUM<>T.CUSTOMER_NUM
 OR N.FLOW_STATUS<>T.FLOW_STATUS
 OR N.HANDLING_USER_NUM<>T.HANDLING_USER_NUM
 OR N.HANDLING_ORG_CD<>T.HANDLING_ORG_CD
 OR N.HANDLING_DATE<>T.HANDLING_DATE
 OR N.FXBS_TIME_MARK<>T.FXBS_TIME_MARK
 OR N.BIZ_PRODUCT_CD<>T.BIZ_PRODUCT_CD
 OR N.CONCLUSION_CD<>T.CONCLUSION_CD
;

--Step3:
UPDATE dw_sdata.CCS_004_TB_AFT_TERM_CHANGE P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_353
WHERE P.End_Dt=DATE('2100-12-31')
AND P.TERM_CHANGE_ID=T_353.TERM_CHANGE_ID
;

--Step4:
INSERT  INTO dw_sdata.CCS_004_TB_AFT_TERM_CHANGE SELECT * FROM T_353;

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
