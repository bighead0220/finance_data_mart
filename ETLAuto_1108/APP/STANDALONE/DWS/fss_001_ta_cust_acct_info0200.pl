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
DELETE FROM dw_sdata.FSS_001_TA_CUST_ACCT_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.FSS_001_TA_CUST_ACCT_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_358 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.FSS_001_TA_CUST_ACCT_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_358 (
  TA_CODE,
  RETAILER_CODE,
  RETAILER_CUST_CODE,
  CUST_TYPE,
  ACCT_STATUS,
  OPEN_DATE,
  OPEN_ORGAN,
  CLOSING_ORGAN,
  CLOSING_DATE,
  start_dt,
  end_dt)
SELECT
  N.TA_CODE,
  N.RETAILER_CODE,
  N.RETAILER_CUST_CODE,
  N.CUST_TYPE,
  N.ACCT_STATUS,
  N.OPEN_DATE,
  N.OPEN_ORGAN,
  N.CLOSING_ORGAN,
  N.CLOSING_DATE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(TA_CODE, '' ) AS TA_CODE ,
  COALESCE(RETAILER_CODE, '' ) AS RETAILER_CODE ,
  COALESCE(RETAILER_CUST_CODE, '' ) AS RETAILER_CUST_CODE ,
  COALESCE(CUST_TYPE, '' ) AS CUST_TYPE ,
  COALESCE(ACCT_STATUS, '' ) AS ACCT_STATUS ,
  COALESCE(OPEN_DATE, '' ) AS OPEN_DATE ,
  COALESCE(OPEN_ORGAN, '' ) AS OPEN_ORGAN ,
  COALESCE(CLOSING_ORGAN, '' ) AS CLOSING_ORGAN ,
  COALESCE(CLOSING_DATE, '' ) AS CLOSING_DATE 
 FROM  dw_tdata.FSS_001_TA_CUST_ACCT_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  TA_CODE ,
  RETAILER_CODE ,
  RETAILER_CUST_CODE ,
  CUST_TYPE ,
  ACCT_STATUS ,
  OPEN_DATE ,
  OPEN_ORGAN ,
  CLOSING_ORGAN ,
  CLOSING_DATE 
 FROM dw_sdata.FSS_001_TA_CUST_ACCT_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.TA_CODE = T.TA_CODE AND N.RETAILER_CODE = T.RETAILER_CODE
WHERE
(T.TA_CODE IS NULL AND T.RETAILER_CODE IS NULL)
 OR N.RETAILER_CUST_CODE<>T.RETAILER_CUST_CODE
 OR N.CUST_TYPE<>T.CUST_TYPE
 OR N.ACCT_STATUS<>T.ACCT_STATUS
 OR N.OPEN_DATE<>T.OPEN_DATE
 OR N.OPEN_ORGAN<>T.OPEN_ORGAN
 OR N.CLOSING_ORGAN<>T.CLOSING_ORGAN
 OR N.CLOSING_DATE<>T.CLOSING_DATE
;

--Step3:
UPDATE dw_sdata.FSS_001_TA_CUST_ACCT_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_358
WHERE P.End_Dt=DATE('2100-12-31')
AND P.TA_CODE=T_358.TA_CODE
AND P.RETAILER_CODE=T_358.RETAILER_CODE
;

--Step4:
INSERT  INTO dw_sdata.FSS_001_TA_CUST_ACCT_INFO SELECT * FROM T_358;

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
