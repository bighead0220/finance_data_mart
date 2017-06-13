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
DELETE FROM dw_sdata.GTS_000_CUST_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.GTS_000_CUST_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_206 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.GTS_000_CUST_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_206 (
  ACCT_NO,
  MEMBER_ID,
  CUST_ID,
  BANK_NO,
  OPEN_BANK_NAME,
  ACCOUNT_NO,
  CUST_NAME,
  CUST_ABBR,
  CERT_TYPE_ID,
  CERT_NUM,
  ACCT_TYPE,
  ACCT_STAT,
  BRANCH_ID,
  GRADE_ID,
  M_FARE_MODEL_ID,
  B_FARE_MODEL_ID,
  EXCH_PWD,
  FUND_PWD,
  OCMA_FLAG,
  OPEN_CHANNEL,
  IS_DEFER_SIGN,
  TRANS_CHECK_INFO,
  PRINT_NUM,
  AFFIRM_DATE,
  O_TERM_TYPE,
  O_TELLER_ID,
  O_DATE,
  M_TERM_TYPE,
  M_TELLER_ID,
  M_DATE,
  start_dt,
  end_dt)
SELECT
  N.ACCT_NO,
  N.MEMBER_ID,
  N.CUST_ID,
  N.BANK_NO,
  N.OPEN_BANK_NAME,
  N.ACCOUNT_NO,
  N.CUST_NAME,
  N.CUST_ABBR,
  N.CERT_TYPE_ID,
  N.CERT_NUM,
  N.ACCT_TYPE,
  N.ACCT_STAT,
  N.BRANCH_ID,
  N.GRADE_ID,
  N.M_FARE_MODEL_ID,
  N.B_FARE_MODEL_ID,
  N.EXCH_PWD,
  N.FUND_PWD,
  N.OCMA_FLAG,
  N.OPEN_CHANNEL,
  N.IS_DEFER_SIGN,
  N.TRANS_CHECK_INFO,
  N.PRINT_NUM,
  N.AFFIRM_DATE,
  N.O_TERM_TYPE,
  N.O_TELLER_ID,
  N.O_DATE,
  N.M_TERM_TYPE,
  N.M_TELLER_ID,
  N.M_DATE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ACCT_NO, '' ) AS ACCT_NO ,
  COALESCE(MEMBER_ID, '' ) AS MEMBER_ID ,
  COALESCE(CUST_ID, '' ) AS CUST_ID ,
  COALESCE(BANK_NO, '' ) AS BANK_NO ,
  COALESCE(OPEN_BANK_NAME, '' ) AS OPEN_BANK_NAME ,
  COALESCE(ACCOUNT_NO, '' ) AS ACCOUNT_NO ,
  COALESCE(CUST_NAME, '' ) AS CUST_NAME ,
  COALESCE(CUST_ABBR, '' ) AS CUST_ABBR ,
  COALESCE(CERT_TYPE_ID, '' ) AS CERT_TYPE_ID ,
  COALESCE(CERT_NUM, '' ) AS CERT_NUM ,
  COALESCE(ACCT_TYPE, '' ) AS ACCT_TYPE ,
  COALESCE(ACCT_STAT, '' ) AS ACCT_STAT ,
  COALESCE(BRANCH_ID, '' ) AS BRANCH_ID ,
  COALESCE(GRADE_ID, '' ) AS GRADE_ID ,
  COALESCE(M_FARE_MODEL_ID, '' ) AS M_FARE_MODEL_ID ,
  COALESCE(B_FARE_MODEL_ID, '' ) AS B_FARE_MODEL_ID ,
  COALESCE(EXCH_PWD, '' ) AS EXCH_PWD ,
  COALESCE(FUND_PWD, '' ) AS FUND_PWD ,
  COALESCE(OCMA_FLAG, '' ) AS OCMA_FLAG ,
  COALESCE(OPEN_CHANNEL, '' ) AS OPEN_CHANNEL ,
  COALESCE(IS_DEFER_SIGN, '' ) AS IS_DEFER_SIGN ,
  COALESCE(TRANS_CHECK_INFO, '' ) AS TRANS_CHECK_INFO ,
  COALESCE(PRINT_NUM, 0 ) AS PRINT_NUM ,
  COALESCE(AFFIRM_DATE, '' ) AS AFFIRM_DATE ,
  COALESCE(O_TERM_TYPE, '' ) AS O_TERM_TYPE ,
  COALESCE(O_TELLER_ID, '' ) AS O_TELLER_ID ,
  COALESCE(O_DATE,DATE('4999-12-31') ) AS O_DATE ,
  COALESCE(M_TERM_TYPE, '' ) AS M_TERM_TYPE ,
  COALESCE(M_TELLER_ID, '' ) AS M_TELLER_ID ,
  COALESCE(M_DATE,DATE('4999-12-31') ) AS M_DATE 
 FROM  dw_tdata.GTS_000_CUST_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ACCT_NO ,
  MEMBER_ID ,
  CUST_ID ,
  BANK_NO ,
  OPEN_BANK_NAME ,
  ACCOUNT_NO ,
  CUST_NAME ,
  CUST_ABBR ,
  CERT_TYPE_ID ,
  CERT_NUM ,
  ACCT_TYPE ,
  ACCT_STAT ,
  BRANCH_ID ,
  GRADE_ID ,
  M_FARE_MODEL_ID ,
  B_FARE_MODEL_ID ,
  EXCH_PWD ,
  FUND_PWD ,
  OCMA_FLAG ,
  OPEN_CHANNEL ,
  IS_DEFER_SIGN ,
  TRANS_CHECK_INFO ,
  PRINT_NUM ,
  AFFIRM_DATE ,
  O_TERM_TYPE ,
  O_TELLER_ID ,
  O_DATE ,
  M_TERM_TYPE ,
  M_TELLER_ID ,
  M_DATE 
 FROM dw_sdata.GTS_000_CUST_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ACCT_NO = T.ACCT_NO
WHERE
(T.ACCT_NO IS NULL)
 OR N.MEMBER_ID<>T.MEMBER_ID
 OR N.CUST_ID<>T.CUST_ID
 OR N.BANK_NO<>T.BANK_NO
 OR N.OPEN_BANK_NAME<>T.OPEN_BANK_NAME
 OR N.ACCOUNT_NO<>T.ACCOUNT_NO
 OR N.CUST_NAME<>T.CUST_NAME
 OR N.CUST_ABBR<>T.CUST_ABBR
 OR N.CERT_TYPE_ID<>T.CERT_TYPE_ID
 OR N.CERT_NUM<>T.CERT_NUM
 OR N.ACCT_TYPE<>T.ACCT_TYPE
 OR N.ACCT_STAT<>T.ACCT_STAT
 OR N.BRANCH_ID<>T.BRANCH_ID
 OR N.GRADE_ID<>T.GRADE_ID
 OR N.M_FARE_MODEL_ID<>T.M_FARE_MODEL_ID
 OR N.B_FARE_MODEL_ID<>T.B_FARE_MODEL_ID
 OR N.EXCH_PWD<>T.EXCH_PWD
 OR N.FUND_PWD<>T.FUND_PWD
 OR N.OCMA_FLAG<>T.OCMA_FLAG
 OR N.OPEN_CHANNEL<>T.OPEN_CHANNEL
 OR N.IS_DEFER_SIGN<>T.IS_DEFER_SIGN
 OR N.TRANS_CHECK_INFO<>T.TRANS_CHECK_INFO
 OR N.PRINT_NUM<>T.PRINT_NUM
 OR N.AFFIRM_DATE<>T.AFFIRM_DATE
 OR N.O_TERM_TYPE<>T.O_TERM_TYPE
 OR N.O_TELLER_ID<>T.O_TELLER_ID
 OR N.O_DATE<>T.O_DATE
 OR N.M_TERM_TYPE<>T.M_TERM_TYPE
 OR N.M_TELLER_ID<>T.M_TELLER_ID
 OR N.M_DATE<>T.M_DATE
;

--Step3:
UPDATE dw_sdata.GTS_000_CUST_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_206
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ACCT_NO=T_206.ACCT_NO
;

--Step4:
INSERT  INTO dw_sdata.GTS_000_CUST_INFO SELECT * FROM T_206;

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
