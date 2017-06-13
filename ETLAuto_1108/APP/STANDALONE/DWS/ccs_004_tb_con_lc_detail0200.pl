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
DELETE FROM dw_sdata.CCS_004_TB_CON_LC_DETAIL WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCS_004_TB_CON_LC_DETAIL SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_112 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCS_004_TB_CON_LC_DETAIL WHERE 1=0;

--Step2:
INSERT  INTO T_112 (
  LONG_DATED_DAY_COUNT,
  LC_NUM,
  SHORT_OVER_LOAD_RATE,
  LC_TYPE,
  OVER_LOAD_RATE,
  BENEFICIARY,
  LONG_DATED_PAYMENT_TERM,
  TERM_UNIT_CD,
  MATURITY_DATE,
  LC_CYCLE_IND,
  LONG_DATED_PAYMENT_TERM_EXP,
  LETTER_CREDIT_CD,
  LETTER_AMT,
  WIDTH_TERM,
  OPEN_LETTER_BANK,
  IS_IMPORT_SUPPLY,
  IMPORT_CONTRACT_NUM,
  LETTER_CREDIT_PAY_TERM,
  CONTRACT_BIZ_DETAIL_ID,
  AMT_FLUCTUATE_PERCENT,
  MAX_LC_AMT,
  BENEFICIARY_ADDRESS,
  LC_FUTURE_TERM,
  IF_HAVE_BILL,
  FXBS_TIME_MARK,
  SALES_CON_CUR_CD,
  SALES_CON_AMT,
  INTERNAL_LC_TYPE,
  LC_OPENING_TYPE,
  HANDLING_BANK,
  HANDLING_ORG_CD,
  HANDLING_BANK_ADDRESS,
  LC_FUTURE_TERM_NUM,
  start_dt,
  end_dt)
SELECT
  N.LONG_DATED_DAY_COUNT,
  N.LC_NUM,
  N.SHORT_OVER_LOAD_RATE,
  N.LC_TYPE,
  N.OVER_LOAD_RATE,
  N.BENEFICIARY,
  N.LONG_DATED_PAYMENT_TERM,
  N.TERM_UNIT_CD,
  N.MATURITY_DATE,
  N.LC_CYCLE_IND,
  N.LONG_DATED_PAYMENT_TERM_EXP,
  N.LETTER_CREDIT_CD,
  N.LETTER_AMT,
  N.WIDTH_TERM,
  N.OPEN_LETTER_BANK,
  N.IS_IMPORT_SUPPLY,
  N.IMPORT_CONTRACT_NUM,
  N.LETTER_CREDIT_PAY_TERM,
  N.CONTRACT_BIZ_DETAIL_ID,
  N.AMT_FLUCTUATE_PERCENT,
  N.MAX_LC_AMT,
  N.BENEFICIARY_ADDRESS,
  N.LC_FUTURE_TERM,
  N.IF_HAVE_BILL,
  N.FXBS_TIME_MARK,
  N.SALES_CON_CUR_CD,
  N.SALES_CON_AMT,
  N.INTERNAL_LC_TYPE,
  N.LC_OPENING_TYPE,
  N.HANDLING_BANK,
  N.HANDLING_ORG_CD,
  N.HANDLING_BANK_ADDRESS,
  N.LC_FUTURE_TERM_NUM,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(LONG_DATED_DAY_COUNT, '' ) AS LONG_DATED_DAY_COUNT ,
  COALESCE(LC_NUM, '' ) AS LC_NUM ,
  COALESCE(SHORT_OVER_LOAD_RATE, 0 ) AS SHORT_OVER_LOAD_RATE ,
  COALESCE(LC_TYPE, '' ) AS LC_TYPE ,
  COALESCE(OVER_LOAD_RATE, 0 ) AS OVER_LOAD_RATE ,
  COALESCE(BENEFICIARY, '' ) AS BENEFICIARY ,
  COALESCE(LONG_DATED_PAYMENT_TERM, 0 ) AS LONG_DATED_PAYMENT_TERM ,
  COALESCE(TERM_UNIT_CD, '' ) AS TERM_UNIT_CD ,
  COALESCE(MATURITY_DATE,'4999-12-31 00:00:00' ) AS MATURITY_DATE ,
  COALESCE(LC_CYCLE_IND, '' ) AS LC_CYCLE_IND ,
  COALESCE(LONG_DATED_PAYMENT_TERM_EXP, '' ) AS LONG_DATED_PAYMENT_TERM_EXP ,
  COALESCE(LETTER_CREDIT_CD, '' ) AS LETTER_CREDIT_CD ,
  COALESCE(LETTER_AMT, 0 ) AS LETTER_AMT ,
  COALESCE(WIDTH_TERM, 0 ) AS WIDTH_TERM ,
  COALESCE(OPEN_LETTER_BANK, '' ) AS OPEN_LETTER_BANK ,
  COALESCE(IS_IMPORT_SUPPLY, '' ) AS IS_IMPORT_SUPPLY ,
  COALESCE(IMPORT_CONTRACT_NUM, '' ) AS IMPORT_CONTRACT_NUM ,
  COALESCE(LETTER_CREDIT_PAY_TERM, 0 ) AS LETTER_CREDIT_PAY_TERM ,
  COALESCE(CONTRACT_BIZ_DETAIL_ID, '' ) AS CONTRACT_BIZ_DETAIL_ID ,
  COALESCE(AMT_FLUCTUATE_PERCENT, 0 ) AS AMT_FLUCTUATE_PERCENT ,
  COALESCE(MAX_LC_AMT, 0 ) AS MAX_LC_AMT ,
  COALESCE(BENEFICIARY_ADDRESS, '' ) AS BENEFICIARY_ADDRESS ,
  COALESCE(LC_FUTURE_TERM, '' ) AS LC_FUTURE_TERM ,
  COALESCE(IF_HAVE_BILL, '' ) AS IF_HAVE_BILL ,
  COALESCE(FXBS_TIME_MARK,'4999-12-31 00:00:00' ) AS FXBS_TIME_MARK ,
  COALESCE(SALES_CON_CUR_CD, '' ) AS SALES_CON_CUR_CD ,
  COALESCE(SALES_CON_AMT, 0 ) AS SALES_CON_AMT ,
  COALESCE(INTERNAL_LC_TYPE, '' ) AS INTERNAL_LC_TYPE ,
  COALESCE(LC_OPENING_TYPE, '' ) AS LC_OPENING_TYPE ,
  COALESCE(HANDLING_BANK, '' ) AS HANDLING_BANK ,
  COALESCE(HANDLING_ORG_CD, '' ) AS HANDLING_ORG_CD ,
  COALESCE(HANDLING_BANK_ADDRESS, '' ) AS HANDLING_BANK_ADDRESS ,
  COALESCE(LC_FUTURE_TERM_NUM, 0 ) AS LC_FUTURE_TERM_NUM 
 FROM  dw_tdata.CCS_004_TB_CON_LC_DETAIL_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  LONG_DATED_DAY_COUNT ,
  LC_NUM ,
  SHORT_OVER_LOAD_RATE ,
  LC_TYPE ,
  OVER_LOAD_RATE ,
  BENEFICIARY ,
  LONG_DATED_PAYMENT_TERM ,
  TERM_UNIT_CD ,
  MATURITY_DATE ,
  LC_CYCLE_IND ,
  LONG_DATED_PAYMENT_TERM_EXP ,
  LETTER_CREDIT_CD ,
  LETTER_AMT ,
  WIDTH_TERM ,
  OPEN_LETTER_BANK ,
  IS_IMPORT_SUPPLY ,
  IMPORT_CONTRACT_NUM ,
  LETTER_CREDIT_PAY_TERM ,
  CONTRACT_BIZ_DETAIL_ID ,
  AMT_FLUCTUATE_PERCENT ,
  MAX_LC_AMT ,
  BENEFICIARY_ADDRESS ,
  LC_FUTURE_TERM ,
  IF_HAVE_BILL ,
  FXBS_TIME_MARK ,
  SALES_CON_CUR_CD ,
  SALES_CON_AMT ,
  INTERNAL_LC_TYPE ,
  LC_OPENING_TYPE ,
  HANDLING_BANK ,
  HANDLING_ORG_CD ,
  HANDLING_BANK_ADDRESS ,
  LC_FUTURE_TERM_NUM 
 FROM dw_sdata.CCS_004_TB_CON_LC_DETAIL 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.LONG_DATED_DAY_COUNT = T.LONG_DATED_DAY_COUNT AND N.CONTRACT_BIZ_DETAIL_ID = T.CONTRACT_BIZ_DETAIL_ID
WHERE
(T.LONG_DATED_DAY_COUNT IS NULL AND T.CONTRACT_BIZ_DETAIL_ID IS NULL)
 OR N.LC_NUM<>T.LC_NUM
 OR N.SHORT_OVER_LOAD_RATE<>T.SHORT_OVER_LOAD_RATE
 OR N.LC_TYPE<>T.LC_TYPE
 OR N.OVER_LOAD_RATE<>T.OVER_LOAD_RATE
 OR N.BENEFICIARY<>T.BENEFICIARY
 OR N.LONG_DATED_PAYMENT_TERM<>T.LONG_DATED_PAYMENT_TERM
 OR N.TERM_UNIT_CD<>T.TERM_UNIT_CD
 OR N.MATURITY_DATE<>T.MATURITY_DATE
 OR N.LC_CYCLE_IND<>T.LC_CYCLE_IND
 OR N.LONG_DATED_PAYMENT_TERM_EXP<>T.LONG_DATED_PAYMENT_TERM_EXP
 OR N.LETTER_CREDIT_CD<>T.LETTER_CREDIT_CD
 OR N.LETTER_AMT<>T.LETTER_AMT
 OR N.WIDTH_TERM<>T.WIDTH_TERM
 OR N.OPEN_LETTER_BANK<>T.OPEN_LETTER_BANK
 OR N.IS_IMPORT_SUPPLY<>T.IS_IMPORT_SUPPLY
 OR N.IMPORT_CONTRACT_NUM<>T.IMPORT_CONTRACT_NUM
 OR N.LETTER_CREDIT_PAY_TERM<>T.LETTER_CREDIT_PAY_TERM
 OR N.AMT_FLUCTUATE_PERCENT<>T.AMT_FLUCTUATE_PERCENT
 OR N.MAX_LC_AMT<>T.MAX_LC_AMT
 OR N.BENEFICIARY_ADDRESS<>T.BENEFICIARY_ADDRESS
 OR N.LC_FUTURE_TERM<>T.LC_FUTURE_TERM
 OR N.IF_HAVE_BILL<>T.IF_HAVE_BILL
 OR N.FXBS_TIME_MARK<>T.FXBS_TIME_MARK
 OR N.SALES_CON_CUR_CD<>T.SALES_CON_CUR_CD
 OR N.SALES_CON_AMT<>T.SALES_CON_AMT
 OR N.INTERNAL_LC_TYPE<>T.INTERNAL_LC_TYPE
 OR N.LC_OPENING_TYPE<>T.LC_OPENING_TYPE
 OR N.HANDLING_BANK<>T.HANDLING_BANK
 OR N.HANDLING_ORG_CD<>T.HANDLING_ORG_CD
 OR N.HANDLING_BANK_ADDRESS<>T.HANDLING_BANK_ADDRESS
 OR N.LC_FUTURE_TERM_NUM<>T.LC_FUTURE_TERM_NUM
;

--Step3:
UPDATE dw_sdata.CCS_004_TB_CON_LC_DETAIL P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_112
WHERE P.End_Dt=DATE('2100-12-31')
AND P.LONG_DATED_DAY_COUNT=T_112.LONG_DATED_DAY_COUNT
AND P.CONTRACT_BIZ_DETAIL_ID=T_112.CONTRACT_BIZ_DETAIL_ID
;

--Step4:
INSERT  INTO dw_sdata.CCS_004_TB_CON_LC_DETAIL SELECT * FROM T_112;

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
