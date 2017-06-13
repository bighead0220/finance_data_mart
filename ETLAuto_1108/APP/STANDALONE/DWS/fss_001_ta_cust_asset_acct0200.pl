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
DELETE FROM dw_sdata.FSS_001_TA_CUST_ASSET_ACCT WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.FSS_001_TA_CUST_ASSET_ACCT SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_360 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.FSS_001_TA_CUST_ASSET_ACCT WHERE 1=0;

--Step2:
INSERT  INTO T_360 (
  TA_CODE,
  PROD_CODE,
  RETAILER_CODE,
  SUM_BUY_AMT,
  SUM_AFFIRM_QUOT,
  TOTAL_QUOT,
  AVAILABLE_QUOT,
  STOP_QUOT,
  FREEZE_QUOT,
  PROFIT_SUM,
  DIVIDEND_FLAG,
  OPEN_DATE,
  ACCRUE_ORGAN,
  CUST_FLAG,
  CURRENCY,
  LATEST_MODIFY_DATE,
  LATEST_MODIFY_TIME,
  start_dt,
  end_dt)
SELECT
  N.TA_CODE,
  N.PROD_CODE,
  N.RETAILER_CODE,
  N.SUM_BUY_AMT,
  N.SUM_AFFIRM_QUOT,
  N.TOTAL_QUOT,
  N.AVAILABLE_QUOT,
  N.STOP_QUOT,
  N.FREEZE_QUOT,
  N.PROFIT_SUM,
  N.DIVIDEND_FLAG,
  N.OPEN_DATE,
  N.ACCRUE_ORGAN,
  N.CUST_FLAG,
  N.CURRENCY,
  N.LATEST_MODIFY_DATE,
  N.LATEST_MODIFY_TIME,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(TA_CODE, '' ) AS TA_CODE ,
  COALESCE(PROD_CODE, '' ) AS PROD_CODE ,
  COALESCE(RETAILER_CODE, '' ) AS RETAILER_CODE ,
  COALESCE(SUM_BUY_AMT, 0 ) AS SUM_BUY_AMT ,
  COALESCE(SUM_AFFIRM_QUOT, 0 ) AS SUM_AFFIRM_QUOT ,
  COALESCE(TOTAL_QUOT, 0 ) AS TOTAL_QUOT ,
  COALESCE(AVAILABLE_QUOT, 0 ) AS AVAILABLE_QUOT ,
  COALESCE(STOP_QUOT, 0 ) AS STOP_QUOT ,
  COALESCE(FREEZE_QUOT, 0 ) AS FREEZE_QUOT ,
  COALESCE(PROFIT_SUM, 0 ) AS PROFIT_SUM ,
  COALESCE(DIVIDEND_FLAG, '' ) AS DIVIDEND_FLAG ,
  COALESCE(OPEN_DATE, '' ) AS OPEN_DATE ,
  COALESCE(ACCRUE_ORGAN, '' ) AS ACCRUE_ORGAN ,
  COALESCE(CUST_FLAG, '' ) AS CUST_FLAG ,
  COALESCE(CURRENCY, '' ) AS CURRENCY ,
  COALESCE(LATEST_MODIFY_DATE, '' ) AS LATEST_MODIFY_DATE ,
  COALESCE(LATEST_MODIFY_TIME, '' ) AS LATEST_MODIFY_TIME 
 FROM  dw_tdata.FSS_001_TA_CUST_ASSET_ACCT_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  TA_CODE ,
  PROD_CODE ,
  RETAILER_CODE ,
  SUM_BUY_AMT ,
  SUM_AFFIRM_QUOT ,
  TOTAL_QUOT ,
  AVAILABLE_QUOT ,
  STOP_QUOT ,
  FREEZE_QUOT ,
  PROFIT_SUM ,
  DIVIDEND_FLAG ,
  OPEN_DATE ,
  ACCRUE_ORGAN ,
  CUST_FLAG ,
  CURRENCY ,
  LATEST_MODIFY_DATE ,
  LATEST_MODIFY_TIME 
 FROM dw_sdata.FSS_001_TA_CUST_ASSET_ACCT 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.TA_CODE = T.TA_CODE AND N.PROD_CODE = T.PROD_CODE AND N.RETAILER_CODE = T.RETAILER_CODE
WHERE
(T.TA_CODE IS NULL AND T.PROD_CODE IS NULL AND T.RETAILER_CODE IS NULL)
 OR N.SUM_BUY_AMT<>T.SUM_BUY_AMT
 OR N.SUM_AFFIRM_QUOT<>T.SUM_AFFIRM_QUOT
 OR N.TOTAL_QUOT<>T.TOTAL_QUOT
 OR N.AVAILABLE_QUOT<>T.AVAILABLE_QUOT
 OR N.STOP_QUOT<>T.STOP_QUOT
 OR N.FREEZE_QUOT<>T.FREEZE_QUOT
 OR N.PROFIT_SUM<>T.PROFIT_SUM
 OR N.DIVIDEND_FLAG<>T.DIVIDEND_FLAG
 OR N.OPEN_DATE<>T.OPEN_DATE
 OR N.ACCRUE_ORGAN<>T.ACCRUE_ORGAN
 OR N.CUST_FLAG<>T.CUST_FLAG
 OR N.CURRENCY<>T.CURRENCY
 OR N.LATEST_MODIFY_DATE<>T.LATEST_MODIFY_DATE
 OR N.LATEST_MODIFY_TIME<>T.LATEST_MODIFY_TIME
;

--Step3:
UPDATE dw_sdata.FSS_001_TA_CUST_ASSET_ACCT P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_360
WHERE P.End_Dt=DATE('2100-12-31')
AND P.TA_CODE=T_360.TA_CODE
AND P.PROD_CODE=T_360.PROD_CODE
AND P.RETAILER_CODE=T_360.RETAILER_CODE
;

--Step4:
INSERT  INTO dw_sdata.FSS_001_TA_CUST_ASSET_ACCT SELECT * FROM T_360;

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
