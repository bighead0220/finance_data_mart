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
DELETE FROM dw_sdata.BBS_001_BAIL WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.BBS_001_BAIL SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_37 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.BBS_001_BAIL WHERE 1=0;

--Step2:
INSERT  INTO T_37 (
  ID,
  CONTRACT_ID,
  SEQ_NO,
  BAIL_ACCOUNT,
  ACCTOUNT_TYPE,
  BAIL_RATE,
  BAIL_CUSTOMER_NAME,
  DEPOSIT_DATE,
  DRAWEE_FLAG,
  CASH_DEPOSIT_TYPE,
  OCCUPATION_AMOUNT,
  MATURITY_DATE,
  CASH_DEPOSTIE_BALANCE,
  MISC,
  LAST_UPD_OPER_ID,
  LAST_UPD_TIME,
  BAIL_CHILD_NO,
  CALCULATE_FLAG,
  CALCULATE_RATE_FLAG,
  BARGAINING_RATE,
  DRIFT_SCAL,
  BAIL_TYPE,
  BAIL_SCALE,
  BALANCE_ACCOUNT,
  BALANCE_ACCOUNT_NAME,
  GUARANTOR_NUM,
  PRINT_COUNT,
  start_dt,
  end_dt)
SELECT
  N.ID,
  N.CONTRACT_ID,
  N.SEQ_NO,
  N.BAIL_ACCOUNT,
  N.ACCTOUNT_TYPE,
  N.BAIL_RATE,
  N.BAIL_CUSTOMER_NAME,
  N.DEPOSIT_DATE,
  N.DRAWEE_FLAG,
  N.CASH_DEPOSIT_TYPE,
  N.OCCUPATION_AMOUNT,
  N.MATURITY_DATE,
  N.CASH_DEPOSTIE_BALANCE,
  N.MISC,
  N.LAST_UPD_OPER_ID,
  N.LAST_UPD_TIME,
  N.BAIL_CHILD_NO,
  N.CALCULATE_FLAG,
  N.CALCULATE_RATE_FLAG,
  N.BARGAINING_RATE,
  N.DRIFT_SCAL,
  N.BAIL_TYPE,
  N.BAIL_SCALE,
  N.BALANCE_ACCOUNT,
  N.BALANCE_ACCOUNT_NAME,
  N.GUARANTOR_NUM,
  N.PRINT_COUNT,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(ID, 0 ) AS ID ,
  COALESCE(CONTRACT_ID, 0 ) AS CONTRACT_ID ,
  COALESCE(SEQ_NO, '' ) AS SEQ_NO ,
  COALESCE(BAIL_ACCOUNT, '' ) AS BAIL_ACCOUNT ,
  COALESCE(ACCTOUNT_TYPE, '' ) AS ACCTOUNT_TYPE ,
  COALESCE(BAIL_RATE, 0 ) AS BAIL_RATE ,
  COALESCE(BAIL_CUSTOMER_NAME, '' ) AS BAIL_CUSTOMER_NAME ,
  COALESCE(DEPOSIT_DATE, '' ) AS DEPOSIT_DATE ,
  COALESCE(DRAWEE_FLAG, '' ) AS DRAWEE_FLAG ,
  COALESCE(CASH_DEPOSIT_TYPE, '' ) AS CASH_DEPOSIT_TYPE ,
  COALESCE(OCCUPATION_AMOUNT, 0 ) AS OCCUPATION_AMOUNT ,
  COALESCE(MATURITY_DATE, '' ) AS MATURITY_DATE ,
  COALESCE(CASH_DEPOSTIE_BALANCE, 0 ) AS CASH_DEPOSTIE_BALANCE ,
  COALESCE(MISC, '' ) AS MISC ,
  COALESCE(LAST_UPD_OPER_ID, 0 ) AS LAST_UPD_OPER_ID ,
  COALESCE(LAST_UPD_TIME, '' ) AS LAST_UPD_TIME ,
  COALESCE(BAIL_CHILD_NO, '' ) AS BAIL_CHILD_NO ,
  COALESCE(CALCULATE_FLAG, '' ) AS CALCULATE_FLAG ,
  COALESCE(CALCULATE_RATE_FLAG, '' ) AS CALCULATE_RATE_FLAG ,
  COALESCE(BARGAINING_RATE, 0 ) AS BARGAINING_RATE ,
  COALESCE(DRIFT_SCAL, 0 ) AS DRIFT_SCAL ,
  COALESCE(BAIL_TYPE, '' ) AS BAIL_TYPE ,
  COALESCE(BAIL_SCALE, 0 ) AS BAIL_SCALE ,
  COALESCE(BALANCE_ACCOUNT, '' ) AS BALANCE_ACCOUNT ,
  COALESCE(BALANCE_ACCOUNT_NAME, '' ) AS BALANCE_ACCOUNT_NAME ,
  COALESCE(GUARANTOR_NUM, 0 ) AS GUARANTOR_NUM ,
  COALESCE(PRINT_COUNT, '' ) AS PRINT_COUNT 
 FROM  dw_tdata.BBS_001_BAIL_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  ID ,
  CONTRACT_ID ,
  SEQ_NO ,
  BAIL_ACCOUNT ,
  ACCTOUNT_TYPE ,
  BAIL_RATE ,
  BAIL_CUSTOMER_NAME ,
  DEPOSIT_DATE ,
  DRAWEE_FLAG ,
  CASH_DEPOSIT_TYPE ,
  OCCUPATION_AMOUNT ,
  MATURITY_DATE ,
  CASH_DEPOSTIE_BALANCE ,
  MISC ,
  LAST_UPD_OPER_ID ,
  LAST_UPD_TIME ,
  BAIL_CHILD_NO ,
  CALCULATE_FLAG ,
  CALCULATE_RATE_FLAG ,
  BARGAINING_RATE ,
  DRIFT_SCAL ,
  BAIL_TYPE ,
  BAIL_SCALE ,
  BALANCE_ACCOUNT ,
  BALANCE_ACCOUNT_NAME ,
  GUARANTOR_NUM ,
  PRINT_COUNT 
 FROM dw_sdata.BBS_001_BAIL 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.ID = T.ID
WHERE
(T.ID IS NULL)
 OR N.CONTRACT_ID<>T.CONTRACT_ID
 OR N.SEQ_NO<>T.SEQ_NO
 OR N.BAIL_ACCOUNT<>T.BAIL_ACCOUNT
 OR N.ACCTOUNT_TYPE<>T.ACCTOUNT_TYPE
 OR N.BAIL_RATE<>T.BAIL_RATE
 OR N.BAIL_CUSTOMER_NAME<>T.BAIL_CUSTOMER_NAME
 OR N.DEPOSIT_DATE<>T.DEPOSIT_DATE
 OR N.DRAWEE_FLAG<>T.DRAWEE_FLAG
 OR N.CASH_DEPOSIT_TYPE<>T.CASH_DEPOSIT_TYPE
 OR N.OCCUPATION_AMOUNT<>T.OCCUPATION_AMOUNT
 OR N.MATURITY_DATE<>T.MATURITY_DATE
 OR N.CASH_DEPOSTIE_BALANCE<>T.CASH_DEPOSTIE_BALANCE
 OR N.MISC<>T.MISC
 OR N.LAST_UPD_OPER_ID<>T.LAST_UPD_OPER_ID
 OR N.LAST_UPD_TIME<>T.LAST_UPD_TIME
 OR N.BAIL_CHILD_NO<>T.BAIL_CHILD_NO
 OR N.CALCULATE_FLAG<>T.CALCULATE_FLAG
 OR N.CALCULATE_RATE_FLAG<>T.CALCULATE_RATE_FLAG
 OR N.BARGAINING_RATE<>T.BARGAINING_RATE
 OR N.DRIFT_SCAL<>T.DRIFT_SCAL
 OR N.BAIL_TYPE<>T.BAIL_TYPE
 OR N.BAIL_SCALE<>T.BAIL_SCALE
 OR N.BALANCE_ACCOUNT<>T.BALANCE_ACCOUNT
 OR N.BALANCE_ACCOUNT_NAME<>T.BALANCE_ACCOUNT_NAME
 OR N.GUARANTOR_NUM<>T.GUARANTOR_NUM
 OR N.PRINT_COUNT<>T.PRINT_COUNT
;

--Step3:
UPDATE dw_sdata.BBS_001_BAIL P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_37
WHERE P.End_Dt=DATE('2100-12-31')
AND P.ID=T_37.ID
;

--Step4:
INSERT  INTO dw_sdata.BBS_001_BAIL SELECT * FROM T_37;

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
   print "Usage: [perl ������ Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
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
   print "Usage: [perl ������ Control_File] (Control_File format: dir.jobnameYYYYMMDD or sysname_jobname_YYYYMMDD.dir) 
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
