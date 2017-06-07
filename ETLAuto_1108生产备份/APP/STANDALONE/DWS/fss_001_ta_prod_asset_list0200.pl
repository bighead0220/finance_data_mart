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
DELETE FROM dw_sdata.FSS_001_TA_PROD_ASSET_LIST WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.FSS_001_TA_PROD_ASSET_LIST SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_359 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.FSS_001_TA_PROD_ASSET_LIST WHERE 1=0;

--Step2:
INSERT  INTO T_359 (
  CONTRACT_NO,
  TA_CODE,
  PROD_CODE,
  RETAILER_CODE,
  CUST_TYPE,
  TOTAL_QUOT,
  AVAILABLE_QUOT,
  STOP_QUOT,
  FREEZE_QUOT,
  CURRENCY,
  DURATION_PERIOD,
  PROFIT_COUNT_DATE,
  ORGAN_CODE,
  start_dt,
  end_dt)
SELECT
  N.CONTRACT_NO,
  N.TA_CODE,
  N.PROD_CODE,
  N.RETAILER_CODE,
  N.CUST_TYPE,
  N.TOTAL_QUOT,
  N.AVAILABLE_QUOT,
  N.STOP_QUOT,
  N.FREEZE_QUOT,
  N.CURRENCY,
  N.DURATION_PERIOD,
  N.PROFIT_COUNT_DATE,
  N.ORGAN_CODE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(CONTRACT_NO, '' ) AS CONTRACT_NO ,
  COALESCE(TA_CODE, '' ) AS TA_CODE ,
  COALESCE(PROD_CODE, '' ) AS PROD_CODE ,
  COALESCE(RETAILER_CODE, '' ) AS RETAILER_CODE ,
  COALESCE(CUST_TYPE, '' ) AS CUST_TYPE ,
  COALESCE(TOTAL_QUOT, 0 ) AS TOTAL_QUOT ,
  COALESCE(AVAILABLE_QUOT, 0 ) AS AVAILABLE_QUOT ,
  COALESCE(STOP_QUOT, 0 ) AS STOP_QUOT ,
  COALESCE(FREEZE_QUOT, 0 ) AS FREEZE_QUOT ,
  COALESCE(CURRENCY, '' ) AS CURRENCY ,
  COALESCE(DURATION_PERIOD, 0 ) AS DURATION_PERIOD ,
  COALESCE(PROFIT_COUNT_DATE, '' ) AS PROFIT_COUNT_DATE ,
  COALESCE(ORGAN_CODE, '' ) AS ORGAN_CODE 
 FROM  dw_tdata.FSS_001_TA_PROD_ASSET_LIST_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  CONTRACT_NO ,
  TA_CODE ,
  PROD_CODE ,
  RETAILER_CODE ,
  CUST_TYPE ,
  TOTAL_QUOT ,
  AVAILABLE_QUOT ,
  STOP_QUOT ,
  FREEZE_QUOT ,
  CURRENCY ,
  DURATION_PERIOD ,
  PROFIT_COUNT_DATE ,
  ORGAN_CODE 
 FROM dw_sdata.FSS_001_TA_PROD_ASSET_LIST 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.CONTRACT_NO = T.CONTRACT_NO AND N.TA_CODE = T.TA_CODE
WHERE
(T.CONTRACT_NO IS NULL AND T.TA_CODE IS NULL)
 OR N.PROD_CODE<>T.PROD_CODE
 OR N.RETAILER_CODE<>T.RETAILER_CODE
 OR N.CUST_TYPE<>T.CUST_TYPE
 OR N.TOTAL_QUOT<>T.TOTAL_QUOT
 OR N.AVAILABLE_QUOT<>T.AVAILABLE_QUOT
 OR N.STOP_QUOT<>T.STOP_QUOT
 OR N.FREEZE_QUOT<>T.FREEZE_QUOT
 OR N.CURRENCY<>T.CURRENCY
 OR N.DURATION_PERIOD<>T.DURATION_PERIOD
 OR N.PROFIT_COUNT_DATE<>T.PROFIT_COUNT_DATE
 OR N.ORGAN_CODE<>T.ORGAN_CODE
;

--Step3:
UPDATE dw_sdata.FSS_001_TA_PROD_ASSET_LIST P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_359
WHERE P.End_Dt=DATE('2100-12-31')
AND P.CONTRACT_NO=T_359.CONTRACT_NO
AND P.TA_CODE=T_359.TA_CODE
;

--Step4:
INSERT  INTO dw_sdata.FSS_001_TA_PROD_ASSET_LIST SELECT * FROM T_359;

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
