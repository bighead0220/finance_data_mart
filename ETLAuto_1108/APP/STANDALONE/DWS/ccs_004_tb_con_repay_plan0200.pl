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
DELETE FROM dw_sdata.CCS_004_TB_CON_REPAY_PLAN WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCS_004_TB_CON_REPAY_PLAN SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_115 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCS_004_TB_CON_REPAY_PLAN WHERE 1=0;

--Step2:
INSERT  INTO T_115 (
  REPAY_PLAN_ID,
  CONTRACT_NUM,
  REPAY_DATE,
  CURRENCY_CD,
  REPAY_AMT,
  CONTRACT_BIZ_DETAIL_ID,
  CUSTOMER_NUM,
  REVERT_TIME,
  REVERT_BALANCE,
  REVERT_SUM,
  ORG_NAME,
  ACCRUAL_DAYS,
  HANDING_DATE,
  PERIODS,
  AVAILABLE_AMT,
  OCCU_AMT,
  SUM_REPAY_AMT,
  BALANCE_REPAY_AMT,
  FXBS_TIME_MARK,
  start_dt,
  end_dt)
SELECT
  N.REPAY_PLAN_ID,
  N.CONTRACT_NUM,
  N.REPAY_DATE,
  N.CURRENCY_CD,
  N.REPAY_AMT,
  N.CONTRACT_BIZ_DETAIL_ID,
  N.CUSTOMER_NUM,
  N.REVERT_TIME,
  N.REVERT_BALANCE,
  N.REVERT_SUM,
  N.ORG_NAME,
  N.ACCRUAL_DAYS,
  N.HANDING_DATE,
  N.PERIODS,
  N.AVAILABLE_AMT,
  N.OCCU_AMT,
  N.SUM_REPAY_AMT,
  N.BALANCE_REPAY_AMT,
  N.FXBS_TIME_MARK,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(REPAY_PLAN_ID, '' ) AS REPAY_PLAN_ID ,
  COALESCE(CONTRACT_NUM, '' ) AS CONTRACT_NUM ,
  COALESCE(REPAY_DATE,'4999-12-31 00:00:00' ) AS REPAY_DATE ,
  COALESCE(CURRENCY_CD, '' ) AS CURRENCY_CD ,
  COALESCE(REPAY_AMT, 0 ) AS REPAY_AMT ,
  COALESCE(CONTRACT_BIZ_DETAIL_ID, '' ) AS CONTRACT_BIZ_DETAIL_ID ,
  COALESCE(CUSTOMER_NUM, '' ) AS CUSTOMER_NUM ,
  COALESCE(REVERT_TIME,'4999-12-31 00:00:00' ) AS REVERT_TIME ,
  COALESCE(REVERT_BALANCE, 0 ) AS REVERT_BALANCE ,
  COALESCE(REVERT_SUM, 0 ) AS REVERT_SUM ,
  COALESCE(ORG_NAME, '' ) AS ORG_NAME ,
  COALESCE(ACCRUAL_DAYS, 0 ) AS ACCRUAL_DAYS ,
  COALESCE(HANDING_DATE,'4999-12-31 00:00:00' ) AS HANDING_DATE ,
  COALESCE(PERIODS, 0 ) AS PERIODS ,
  COALESCE(AVAILABLE_AMT, 0 ) AS AVAILABLE_AMT ,
  COALESCE(OCCU_AMT, 0 ) AS OCCU_AMT ,
  COALESCE(SUM_REPAY_AMT, 0 ) AS SUM_REPAY_AMT ,
  COALESCE(BALANCE_REPAY_AMT, 0 ) AS BALANCE_REPAY_AMT ,
  COALESCE(FXBS_TIME_MARK,'4999-12-31 00:00:00' ) AS FXBS_TIME_MARK 
 FROM  dw_tdata.CCS_004_TB_CON_REPAY_PLAN_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  REPAY_PLAN_ID ,
  CONTRACT_NUM ,
  REPAY_DATE ,
  CURRENCY_CD ,
  REPAY_AMT ,
  CONTRACT_BIZ_DETAIL_ID ,
  CUSTOMER_NUM ,
  REVERT_TIME ,
  REVERT_BALANCE ,
  REVERT_SUM ,
  ORG_NAME ,
  ACCRUAL_DAYS ,
  HANDING_DATE ,
  PERIODS ,
  AVAILABLE_AMT ,
  OCCU_AMT ,
  SUM_REPAY_AMT ,
  BALANCE_REPAY_AMT ,
  FXBS_TIME_MARK 
 FROM dw_sdata.CCS_004_TB_CON_REPAY_PLAN 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.REPAY_PLAN_ID = T.REPAY_PLAN_ID
WHERE
(T.REPAY_PLAN_ID IS NULL)
 OR N.CONTRACT_NUM<>T.CONTRACT_NUM
 OR N.REPAY_DATE<>T.REPAY_DATE
 OR N.CURRENCY_CD<>T.CURRENCY_CD
 OR N.REPAY_AMT<>T.REPAY_AMT
 OR N.CONTRACT_BIZ_DETAIL_ID<>T.CONTRACT_BIZ_DETAIL_ID
 OR N.CUSTOMER_NUM<>T.CUSTOMER_NUM
 OR N.REVERT_TIME<>T.REVERT_TIME
 OR N.REVERT_BALANCE<>T.REVERT_BALANCE
 OR N.REVERT_SUM<>T.REVERT_SUM
 OR N.ORG_NAME<>T.ORG_NAME
 OR N.ACCRUAL_DAYS<>T.ACCRUAL_DAYS
 OR N.HANDING_DATE<>T.HANDING_DATE
 OR N.PERIODS<>T.PERIODS
 OR N.AVAILABLE_AMT<>T.AVAILABLE_AMT
 OR N.OCCU_AMT<>T.OCCU_AMT
 OR N.SUM_REPAY_AMT<>T.SUM_REPAY_AMT
 OR N.BALANCE_REPAY_AMT<>T.BALANCE_REPAY_AMT
 OR N.FXBS_TIME_MARK<>T.FXBS_TIME_MARK
;

--Step3:
UPDATE dw_sdata.CCS_004_TB_CON_REPAY_PLAN P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_115
WHERE P.End_Dt=DATE('2100-12-31')
AND P.REPAY_PLAN_ID=T_115.REPAY_PLAN_ID
;

--Step4:
INSERT  INTO dw_sdata.CCS_004_TB_CON_REPAY_PLAN SELECT * FROM T_115;

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
