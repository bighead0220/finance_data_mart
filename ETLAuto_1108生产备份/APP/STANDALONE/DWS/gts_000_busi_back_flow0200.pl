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
DELETE FROM dw_sdata.GTS_000_BUSI_BACK_FLOW WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.GTS_000_BUSI_BACK_FLOW SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_204 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.GTS_000_BUSI_BACK_FLOW WHERE 1=0;

--Step2:
INSERT  INTO T_204 (
  MATCH_NO,
  ORDER_NO,
  LOCAL_ORDER_NO,
  MEMBER_ID,
  ACCT_NO,
  CUST_ID,
  CLIENT_TYPE,
  MARKET_ID,
  EXCH_CODE,
  EXCH_DATE,
  EXCH_TIME,
  PROD_CODE,
  MATCH_PRICE,
  MATCH_AMOUNT,
  BS,
  OFFSET_FLAG,
  DELI_FLAG,
  EXCH_BAL,
  B_EXCH_FARE,
  M_EXCH_FARE,
  B_MARGIN,
  M_MARGIN,
  MATCH_TYPE,
  COV_SURPLUS,
  BROKER_ACCT_NO,
  TERM_TYPE,
  BRANCH_ID,
  TELLER_ID,
  start_dt,
  end_dt)
SELECT
  N.MATCH_NO,
  N.ORDER_NO,
  N.LOCAL_ORDER_NO,
  N.MEMBER_ID,
  N.ACCT_NO,
  N.CUST_ID,
  N.CLIENT_TYPE,
  N.MARKET_ID,
  N.EXCH_CODE,
  N.EXCH_DATE,
  N.EXCH_TIME,
  N.PROD_CODE,
  N.MATCH_PRICE,
  N.MATCH_AMOUNT,
  N.BS,
  N.OFFSET_FLAG,
  N.DELI_FLAG,
  N.EXCH_BAL,
  N.B_EXCH_FARE,
  N.M_EXCH_FARE,
  N.B_MARGIN,
  N.M_MARGIN,
  N.MATCH_TYPE,
  N.COV_SURPLUS,
  N.BROKER_ACCT_NO,
  N.TERM_TYPE,
  N.BRANCH_ID,
  N.TELLER_ID,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(MATCH_NO, '' ) AS MATCH_NO ,
  COALESCE(ORDER_NO, '' ) AS ORDER_NO ,
  COALESCE(LOCAL_ORDER_NO, '' ) AS LOCAL_ORDER_NO ,
  COALESCE(MEMBER_ID, '' ) AS MEMBER_ID ,
  COALESCE(ACCT_NO, '' ) AS ACCT_NO ,
  COALESCE(CUST_ID, '' ) AS CUST_ID ,
  COALESCE(CLIENT_TYPE, '' ) AS CLIENT_TYPE ,
  COALESCE(MARKET_ID, '' ) AS MARKET_ID ,
  COALESCE(EXCH_CODE, '' ) AS EXCH_CODE ,
  COALESCE(EXCH_DATE, '' ) AS EXCH_DATE ,
  COALESCE(EXCH_TIME, '' ) AS EXCH_TIME ,
  COALESCE(PROD_CODE, '' ) AS PROD_CODE ,
  COALESCE(MATCH_PRICE, 0 ) AS MATCH_PRICE ,
  COALESCE(MATCH_AMOUNT, 0 ) AS MATCH_AMOUNT ,
  COALESCE(BS, '' ) AS BS ,
  COALESCE(OFFSET_FLAG, '' ) AS OFFSET_FLAG ,
  COALESCE(DELI_FLAG, '' ) AS DELI_FLAG ,
  COALESCE(EXCH_BAL, 0 ) AS EXCH_BAL ,
  COALESCE(B_EXCH_FARE, 0 ) AS B_EXCH_FARE ,
  COALESCE(M_EXCH_FARE, 0 ) AS M_EXCH_FARE ,
  COALESCE(B_MARGIN, 0 ) AS B_MARGIN ,
  COALESCE(M_MARGIN, 0 ) AS M_MARGIN ,
  COALESCE(MATCH_TYPE, '' ) AS MATCH_TYPE ,
  COALESCE(COV_SURPLUS, 0 ) AS COV_SURPLUS ,
  COALESCE(BROKER_ACCT_NO, '' ) AS BROKER_ACCT_NO ,
  COALESCE(TERM_TYPE, '' ) AS TERM_TYPE ,
  COALESCE(BRANCH_ID, '' ) AS BRANCH_ID ,
  COALESCE(TELLER_ID, '' ) AS TELLER_ID 
 FROM  dw_tdata.GTS_000_BUSI_BACK_FLOW_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  MATCH_NO ,
  ORDER_NO ,
  LOCAL_ORDER_NO ,
  MEMBER_ID ,
  ACCT_NO ,
  CUST_ID ,
  CLIENT_TYPE ,
  MARKET_ID ,
  EXCH_CODE ,
  EXCH_DATE ,
  EXCH_TIME ,
  PROD_CODE ,
  MATCH_PRICE ,
  MATCH_AMOUNT ,
  BS ,
  OFFSET_FLAG ,
  DELI_FLAG ,
  EXCH_BAL ,
  B_EXCH_FARE ,
  M_EXCH_FARE ,
  B_MARGIN ,
  M_MARGIN ,
  MATCH_TYPE ,
  COV_SURPLUS ,
  BROKER_ACCT_NO ,
  TERM_TYPE ,
  BRANCH_ID ,
  TELLER_ID 
 FROM dw_sdata.GTS_000_BUSI_BACK_FLOW 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.MATCH_NO = T.MATCH_NO AND N.ORDER_NO = T.ORDER_NO
WHERE
(T.MATCH_NO IS NULL AND T.ORDER_NO IS NULL)
 OR N.LOCAL_ORDER_NO<>T.LOCAL_ORDER_NO
 OR N.MEMBER_ID<>T.MEMBER_ID
 OR N.ACCT_NO<>T.ACCT_NO
 OR N.CUST_ID<>T.CUST_ID
 OR N.CLIENT_TYPE<>T.CLIENT_TYPE
 OR N.MARKET_ID<>T.MARKET_ID
 OR N.EXCH_CODE<>T.EXCH_CODE
 OR N.EXCH_DATE<>T.EXCH_DATE
 OR N.EXCH_TIME<>T.EXCH_TIME
 OR N.PROD_CODE<>T.PROD_CODE
 OR N.MATCH_PRICE<>T.MATCH_PRICE
 OR N.MATCH_AMOUNT<>T.MATCH_AMOUNT
 OR N.BS<>T.BS
 OR N.OFFSET_FLAG<>T.OFFSET_FLAG
 OR N.DELI_FLAG<>T.DELI_FLAG
 OR N.EXCH_BAL<>T.EXCH_BAL
 OR N.B_EXCH_FARE<>T.B_EXCH_FARE
 OR N.M_EXCH_FARE<>T.M_EXCH_FARE
 OR N.B_MARGIN<>T.B_MARGIN
 OR N.M_MARGIN<>T.M_MARGIN
 OR N.MATCH_TYPE<>T.MATCH_TYPE
 OR N.COV_SURPLUS<>T.COV_SURPLUS
 OR N.BROKER_ACCT_NO<>T.BROKER_ACCT_NO
 OR N.TERM_TYPE<>T.TERM_TYPE
 OR N.BRANCH_ID<>T.BRANCH_ID
 OR N.TELLER_ID<>T.TELLER_ID
;

--Step3:
UPDATE dw_sdata.GTS_000_BUSI_BACK_FLOW P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_204
WHERE P.End_Dt=DATE('2100-12-31')
AND P.MATCH_NO=T_204.MATCH_NO
AND P.ORDER_NO=T_204.ORDER_NO
;

--Step4:
INSERT  INTO dw_sdata.GTS_000_BUSI_BACK_FLOW SELECT * FROM T_204;

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
