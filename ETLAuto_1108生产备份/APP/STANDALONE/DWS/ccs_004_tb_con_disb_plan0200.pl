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
DELETE FROM dw_sdata.CCS_004_TB_CON_DISB_PLAN WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCS_004_TB_CON_DISB_PLAN SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_111 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCS_004_TB_CON_DISB_PLAN WHERE 1=0;

--Step2:
INSERT  INTO T_111 (
  PAYOUT_PLAN_ID,
  CONTRACT_NUM,
  PAYOUT_DATE,
  EXPIRATION_DATE,
  AMT,
  CONTRACT_BIZ_DETAIL_ID,
  CURRENCY_CD,
  CUSTOMER_NUM,
  DRAW_TIME,
  DRAW_BALANCE,
  SUM_DRAW_BALANCE,
  HANDING_DATE,
  PERIODS,
  SUM_DRAW_AMT,
  BALANCE_DRAW_AMT,
  FXBS_TIME_MARK,
  start_dt,
  end_dt)
SELECT
  N.PAYOUT_PLAN_ID,
  N.CONTRACT_NUM,
  N.PAYOUT_DATE,
  N.EXPIRATION_DATE,
  N.AMT,
  N.CONTRACT_BIZ_DETAIL_ID,
  N.CURRENCY_CD,
  N.CUSTOMER_NUM,
  N.DRAW_TIME,
  N.DRAW_BALANCE,
  N.SUM_DRAW_BALANCE,
  N.HANDING_DATE,
  N.PERIODS,
  N.SUM_DRAW_AMT,
  N.BALANCE_DRAW_AMT,
  N.FXBS_TIME_MARK,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(PAYOUT_PLAN_ID, '' ) AS PAYOUT_PLAN_ID ,
  COALESCE(CONTRACT_NUM, '' ) AS CONTRACT_NUM ,
  COALESCE(PAYOUT_DATE,'4999-12-31 00:00:00' ) AS PAYOUT_DATE ,
  COALESCE(EXPIRATION_DATE,'4999-12-31 00:00:00' ) AS EXPIRATION_DATE ,
  COALESCE(AMT, 0 ) AS AMT ,
  COALESCE(CONTRACT_BIZ_DETAIL_ID, '' ) AS CONTRACT_BIZ_DETAIL_ID ,
  COALESCE(CURRENCY_CD, '' ) AS CURRENCY_CD ,
  COALESCE(CUSTOMER_NUM, '' ) AS CUSTOMER_NUM ,
  COALESCE(DRAW_TIME,'4999-12-31 00:00:00' ) AS DRAW_TIME ,
  COALESCE(DRAW_BALANCE, 0 ) AS DRAW_BALANCE ,
  COALESCE(SUM_DRAW_BALANCE, 0 ) AS SUM_DRAW_BALANCE ,
  COALESCE(HANDING_DATE,'4999-12-31 00:00:00' ) AS HANDING_DATE ,
  COALESCE(PERIODS, 0 ) AS PERIODS ,
  COALESCE(SUM_DRAW_AMT, 0 ) AS SUM_DRAW_AMT ,
  COALESCE(BALANCE_DRAW_AMT, 0 ) AS BALANCE_DRAW_AMT ,
  COALESCE(FXBS_TIME_MARK,'4999-12-31 00:00:00' ) AS FXBS_TIME_MARK 
 FROM  dw_tdata.CCS_004_TB_CON_DISB_PLAN_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  PAYOUT_PLAN_ID ,
  CONTRACT_NUM ,
  PAYOUT_DATE ,
  EXPIRATION_DATE ,
  AMT ,
  CONTRACT_BIZ_DETAIL_ID ,
  CURRENCY_CD ,
  CUSTOMER_NUM ,
  DRAW_TIME ,
  DRAW_BALANCE ,
  SUM_DRAW_BALANCE ,
  HANDING_DATE ,
  PERIODS ,
  SUM_DRAW_AMT ,
  BALANCE_DRAW_AMT ,
  FXBS_TIME_MARK 
 FROM dw_sdata.CCS_004_TB_CON_DISB_PLAN 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.PAYOUT_PLAN_ID = T.PAYOUT_PLAN_ID
WHERE
(T.PAYOUT_PLAN_ID IS NULL)
 OR N.CONTRACT_NUM<>T.CONTRACT_NUM
 OR N.PAYOUT_DATE<>T.PAYOUT_DATE
 OR N.EXPIRATION_DATE<>T.EXPIRATION_DATE
 OR N.AMT<>T.AMT
 OR N.CONTRACT_BIZ_DETAIL_ID<>T.CONTRACT_BIZ_DETAIL_ID
 OR N.CURRENCY_CD<>T.CURRENCY_CD
 OR N.CUSTOMER_NUM<>T.CUSTOMER_NUM
 OR N.DRAW_TIME<>T.DRAW_TIME
 OR N.DRAW_BALANCE<>T.DRAW_BALANCE
 OR N.SUM_DRAW_BALANCE<>T.SUM_DRAW_BALANCE
 OR N.HANDING_DATE<>T.HANDING_DATE
 OR N.PERIODS<>T.PERIODS
 OR N.SUM_DRAW_AMT<>T.SUM_DRAW_AMT
 OR N.BALANCE_DRAW_AMT<>T.BALANCE_DRAW_AMT
 OR N.FXBS_TIME_MARK<>T.FXBS_TIME_MARK
;

--Step3:
UPDATE dw_sdata.CCS_004_TB_CON_DISB_PLAN P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_111
WHERE P.End_Dt=DATE('2100-12-31')
AND P.PAYOUT_PLAN_ID=T_111.PAYOUT_PLAN_ID
;

--Step4:
INSERT  INTO dw_sdata.CCS_004_TB_CON_DISB_PLAN SELECT * FROM T_111;

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
