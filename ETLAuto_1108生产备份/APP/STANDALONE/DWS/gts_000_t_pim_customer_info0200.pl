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
DELETE FROM dw_sdata.GTS_000_T_PIM_CUSTOMER_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.GTS_000_T_PIM_CUSTOMER_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_213 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.GTS_000_T_PIM_CUSTOMER_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_213 (
  CUSTOMER_ID,
  CUSTOMER_FULL_NAME,
  CUSTOMER_NAME,
  CERT_TYPE,
  CERT_NUM,
  CUSTOMER_TYPE,
  CUSTOMER_STATE,
  CUSTOMER_LEVEL,
  LEVEL_EXPIRE_DATE,
  EXCH_PWD,
  FUND_PWD,
  SIGN_BRANCH_ID,
  RISK_LEVEL_ID,
  RISK_LEVEL_EXPIRE_DATE,
  O_TERM_TYPE,
  O_TELLER,
  O_DATE,
  M_TERM_TYPE,
  M_TELLER,
  M_DATE,
  BANK_NO,
  OPEN_BANK_NAME,
  GOLD_EXCH_NO,
  M_DATE_PWD,
  start_dt,
  end_dt)
SELECT
  N.CUSTOMER_ID,
  N.CUSTOMER_FULL_NAME,
  N.CUSTOMER_NAME,
  N.CERT_TYPE,
  N.CERT_NUM,
  N.CUSTOMER_TYPE,
  N.CUSTOMER_STATE,
  N.CUSTOMER_LEVEL,
  N.LEVEL_EXPIRE_DATE,
  N.EXCH_PWD,
  N.FUND_PWD,
  N.SIGN_BRANCH_ID,
  N.RISK_LEVEL_ID,
  N.RISK_LEVEL_EXPIRE_DATE,
  N.O_TERM_TYPE,
  N.O_TELLER,
  N.O_DATE,
  N.M_TERM_TYPE,
  N.M_TELLER,
  N.M_DATE,
  N.BANK_NO,
  N.OPEN_BANK_NAME,
  N.GOLD_EXCH_NO,
  N.M_DATE_PWD,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(CUSTOMER_ID, '' ) AS CUSTOMER_ID ,
  COALESCE(CUSTOMER_FULL_NAME, '' ) AS CUSTOMER_FULL_NAME ,
  COALESCE(CUSTOMER_NAME, '' ) AS CUSTOMER_NAME ,
  COALESCE(CERT_TYPE, '' ) AS CERT_TYPE ,
  COALESCE(CERT_NUM, '' ) AS CERT_NUM ,
  COALESCE(CUSTOMER_TYPE, '' ) AS CUSTOMER_TYPE ,
  COALESCE(CUSTOMER_STATE, '' ) AS CUSTOMER_STATE ,
  COALESCE(CUSTOMER_LEVEL, '' ) AS CUSTOMER_LEVEL ,
  COALESCE(LEVEL_EXPIRE_DATE, '' ) AS LEVEL_EXPIRE_DATE ,
  COALESCE(EXCH_PWD, '' ) AS EXCH_PWD ,
  COALESCE(FUND_PWD, '' ) AS FUND_PWD ,
  COALESCE(SIGN_BRANCH_ID, '' ) AS SIGN_BRANCH_ID ,
  COALESCE(RISK_LEVEL_ID, '' ) AS RISK_LEVEL_ID ,
  COALESCE(RISK_LEVEL_EXPIRE_DATE, '' ) AS RISK_LEVEL_EXPIRE_DATE ,
  COALESCE(O_TERM_TYPE, '' ) AS O_TERM_TYPE ,
  COALESCE(O_TELLER, '' ) AS O_TELLER ,
  COALESCE(O_DATE, '' ) AS O_DATE ,
  COALESCE(M_TERM_TYPE, '' ) AS M_TERM_TYPE ,
  COALESCE(M_TELLER, '' ) AS M_TELLER ,
  COALESCE(M_DATE, '' ) AS M_DATE ,
  COALESCE(BANK_NO, '' ) AS BANK_NO ,
  COALESCE(OPEN_BANK_NAME, '' ) AS OPEN_BANK_NAME ,
  COALESCE(GOLD_EXCH_NO, '' ) AS GOLD_EXCH_NO ,
  COALESCE(M_DATE_PWD, '' ) AS M_DATE_PWD 
 FROM  dw_tdata.GTS_000_T_PIM_CUSTOMER_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  CUSTOMER_ID ,
  CUSTOMER_FULL_NAME ,
  CUSTOMER_NAME ,
  CERT_TYPE ,
  CERT_NUM ,
  CUSTOMER_TYPE ,
  CUSTOMER_STATE ,
  CUSTOMER_LEVEL ,
  LEVEL_EXPIRE_DATE ,
  EXCH_PWD ,
  FUND_PWD ,
  SIGN_BRANCH_ID ,
  RISK_LEVEL_ID ,
  RISK_LEVEL_EXPIRE_DATE ,
  O_TERM_TYPE ,
  O_TELLER ,
  O_DATE ,
  M_TERM_TYPE ,
  M_TELLER ,
  M_DATE ,
  BANK_NO ,
  OPEN_BANK_NAME ,
  GOLD_EXCH_NO ,
  M_DATE_PWD 
 FROM dw_sdata.GTS_000_T_PIM_CUSTOMER_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.CUSTOMER_ID = T.CUSTOMER_ID
WHERE
(T.CUSTOMER_ID IS NULL)
 OR N.CUSTOMER_FULL_NAME<>T.CUSTOMER_FULL_NAME
 OR N.CUSTOMER_NAME<>T.CUSTOMER_NAME
 OR N.CERT_TYPE<>T.CERT_TYPE
 OR N.CERT_NUM<>T.CERT_NUM
 OR N.CUSTOMER_TYPE<>T.CUSTOMER_TYPE
 OR N.CUSTOMER_STATE<>T.CUSTOMER_STATE
 OR N.CUSTOMER_LEVEL<>T.CUSTOMER_LEVEL
 OR N.LEVEL_EXPIRE_DATE<>T.LEVEL_EXPIRE_DATE
 OR N.EXCH_PWD<>T.EXCH_PWD
 OR N.FUND_PWD<>T.FUND_PWD
 OR N.SIGN_BRANCH_ID<>T.SIGN_BRANCH_ID
 OR N.RISK_LEVEL_ID<>T.RISK_LEVEL_ID
 OR N.RISK_LEVEL_EXPIRE_DATE<>T.RISK_LEVEL_EXPIRE_DATE
 OR N.O_TERM_TYPE<>T.O_TERM_TYPE
 OR N.O_TELLER<>T.O_TELLER
 OR N.O_DATE<>T.O_DATE
 OR N.M_TERM_TYPE<>T.M_TERM_TYPE
 OR N.M_TELLER<>T.M_TELLER
 OR N.M_DATE<>T.M_DATE
 OR N.BANK_NO<>T.BANK_NO
 OR N.OPEN_BANK_NAME<>T.OPEN_BANK_NAME
 OR N.GOLD_EXCH_NO<>T.GOLD_EXCH_NO
 OR N.M_DATE_PWD<>T.M_DATE_PWD
;

--Step3:
UPDATE dw_sdata.GTS_000_T_PIM_CUSTOMER_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_213
WHERE P.End_Dt=DATE('2100-12-31')
AND P.CUSTOMER_ID=T_213.CUSTOMER_ID
;

--Step4:
INSERT  INTO dw_sdata.GTS_000_T_PIM_CUSTOMER_INFO SELECT * FROM T_213;

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
