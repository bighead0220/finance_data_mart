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
DELETE FROM dw_sdata.CCS_004_TB_SYS_PRODUCT WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCS_004_TB_SYS_PRODUCT SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_124 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCS_004_TB_SYS_PRODUCT WHERE 1=0;

--Step2:
INSERT  INTO T_124 (
  PRODUCT_ID,
  PRODUCT_BELONG_CD,
  PRODUCT_CD,
  PRODUCT_NAME,
  PRODUCT_LEVEL,
  CREDIT_PRODUCT_RISK_RATE_CD,
  CONVERT_COEFFICIENT,
  CIRCLE_IND,
  REMARKS,
  CURRENCY_DIFFERENCE_CD,
  DISCOUNT_IND,
  OUTSTAND_LIMIT_RATE,
  ACCOUNTING_ATTACH_CD,
  CUSTOMER_TYPE_CD,
  INDUSTRY_TYPE_CD,
  CUSTOMER_SIZE_CD,
  CALC_INTEREST_IND,
  LOW_RISK_IND,
  LIMIT_LOGIN_OCCASION_CD,
  LIMIT_LOGIN_AMOUNT,
  SUPERIOR_ID,
  SUPERIOR_CD,
  CHARGE_IND,
  PRODUCT_KIND_CD,
  SPECIALTY,
  RSP_CODE,
  RSP_TIME,
  FXBS_TIME_MARK,
  BUSINESS_MODE,
  FINACING_MODE,
  start_dt,
  end_dt)
SELECT
  N.PRODUCT_ID,
  N.PRODUCT_BELONG_CD,
  N.PRODUCT_CD,
  N.PRODUCT_NAME,
  N.PRODUCT_LEVEL,
  N.CREDIT_PRODUCT_RISK_RATE_CD,
  N.CONVERT_COEFFICIENT,
  N.CIRCLE_IND,
  N.REMARKS,
  N.CURRENCY_DIFFERENCE_CD,
  N.DISCOUNT_IND,
  N.OUTSTAND_LIMIT_RATE,
  N.ACCOUNTING_ATTACH_CD,
  N.CUSTOMER_TYPE_CD,
  N.INDUSTRY_TYPE_CD,
  N.CUSTOMER_SIZE_CD,
  N.CALC_INTEREST_IND,
  N.LOW_RISK_IND,
  N.LIMIT_LOGIN_OCCASION_CD,
  N.LIMIT_LOGIN_AMOUNT,
  N.SUPERIOR_ID,
  N.SUPERIOR_CD,
  N.CHARGE_IND,
  N.PRODUCT_KIND_CD,
  N.SPECIALTY,
  N.RSP_CODE,
  N.RSP_TIME,
  N.FXBS_TIME_MARK,
  N.BUSINESS_MODE,
  N.FINACING_MODE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(PRODUCT_ID, '' ) AS PRODUCT_ID ,
  COALESCE(PRODUCT_BELONG_CD, '' ) AS PRODUCT_BELONG_CD ,
  COALESCE(PRODUCT_CD, '' ) AS PRODUCT_CD ,
  COALESCE(PRODUCT_NAME, '' ) AS PRODUCT_NAME ,
  COALESCE(PRODUCT_LEVEL, '' ) AS PRODUCT_LEVEL ,
  COALESCE(CREDIT_PRODUCT_RISK_RATE_CD, '' ) AS CREDIT_PRODUCT_RISK_RATE_CD ,
  COALESCE(CONVERT_COEFFICIENT, 0 ) AS CONVERT_COEFFICIENT ,
  COALESCE(CIRCLE_IND, '' ) AS CIRCLE_IND ,
  COALESCE(REMARKS, '' ) AS REMARKS ,
  COALESCE(CURRENCY_DIFFERENCE_CD, '' ) AS CURRENCY_DIFFERENCE_CD ,
  COALESCE(DISCOUNT_IND, '' ) AS DISCOUNT_IND ,
  COALESCE(OUTSTAND_LIMIT_RATE, '' ) AS OUTSTAND_LIMIT_RATE ,
  COALESCE(ACCOUNTING_ATTACH_CD, '' ) AS ACCOUNTING_ATTACH_CD ,
  COALESCE(CUSTOMER_TYPE_CD, '' ) AS CUSTOMER_TYPE_CD ,
  COALESCE(INDUSTRY_TYPE_CD, '' ) AS INDUSTRY_TYPE_CD ,
  COALESCE(CUSTOMER_SIZE_CD, '' ) AS CUSTOMER_SIZE_CD ,
  COALESCE(CALC_INTEREST_IND, '' ) AS CALC_INTEREST_IND ,
  COALESCE(LOW_RISK_IND, '' ) AS LOW_RISK_IND ,
  COALESCE(LIMIT_LOGIN_OCCASION_CD, '' ) AS LIMIT_LOGIN_OCCASION_CD ,
  COALESCE(LIMIT_LOGIN_AMOUNT, 0 ) AS LIMIT_LOGIN_AMOUNT ,
  COALESCE(SUPERIOR_ID, '' ) AS SUPERIOR_ID ,
  COALESCE(SUPERIOR_CD, '' ) AS SUPERIOR_CD ,
  COALESCE(CHARGE_IND, '' ) AS CHARGE_IND ,
  COALESCE(PRODUCT_KIND_CD, '' ) AS PRODUCT_KIND_CD ,
  COALESCE(SPECIALTY, '' ) AS SPECIALTY ,
  COALESCE(RSP_CODE, '' ) AS RSP_CODE ,
  COALESCE(RSP_TIME,'4999-12-31 00:00:00' ) AS RSP_TIME ,
  COALESCE(FXBS_TIME_MARK,'4999-12-31 00:00:00' ) AS FXBS_TIME_MARK ,
  COALESCE(BUSINESS_MODE, '' ) AS BUSINESS_MODE ,
  COALESCE(FINACING_MODE, '' ) AS FINACING_MODE 
 FROM  dw_tdata.CCS_004_TB_SYS_PRODUCT_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  PRODUCT_ID ,
  PRODUCT_BELONG_CD ,
  PRODUCT_CD ,
  PRODUCT_NAME ,
  PRODUCT_LEVEL ,
  CREDIT_PRODUCT_RISK_RATE_CD ,
  CONVERT_COEFFICIENT ,
  CIRCLE_IND ,
  REMARKS ,
  CURRENCY_DIFFERENCE_CD ,
  DISCOUNT_IND ,
  OUTSTAND_LIMIT_RATE ,
  ACCOUNTING_ATTACH_CD ,
  CUSTOMER_TYPE_CD ,
  INDUSTRY_TYPE_CD ,
  CUSTOMER_SIZE_CD ,
  CALC_INTEREST_IND ,
  LOW_RISK_IND ,
  LIMIT_LOGIN_OCCASION_CD ,
  LIMIT_LOGIN_AMOUNT ,
  SUPERIOR_ID ,
  SUPERIOR_CD ,
  CHARGE_IND ,
  PRODUCT_KIND_CD ,
  SPECIALTY ,
  RSP_CODE ,
  RSP_TIME ,
  FXBS_TIME_MARK ,
  BUSINESS_MODE ,
  FINACING_MODE 
 FROM dw_sdata.CCS_004_TB_SYS_PRODUCT 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.PRODUCT_ID = T.PRODUCT_ID
WHERE
(T.PRODUCT_ID IS NULL)
 OR N.PRODUCT_BELONG_CD<>T.PRODUCT_BELONG_CD
 OR N.PRODUCT_CD<>T.PRODUCT_CD
 OR N.PRODUCT_NAME<>T.PRODUCT_NAME
 OR N.PRODUCT_LEVEL<>T.PRODUCT_LEVEL
 OR N.CREDIT_PRODUCT_RISK_RATE_CD<>T.CREDIT_PRODUCT_RISK_RATE_CD
 OR N.CONVERT_COEFFICIENT<>T.CONVERT_COEFFICIENT
 OR N.CIRCLE_IND<>T.CIRCLE_IND
 OR N.REMARKS<>T.REMARKS
 OR N.CURRENCY_DIFFERENCE_CD<>T.CURRENCY_DIFFERENCE_CD
 OR N.DISCOUNT_IND<>T.DISCOUNT_IND
 OR N.OUTSTAND_LIMIT_RATE<>T.OUTSTAND_LIMIT_RATE
 OR N.ACCOUNTING_ATTACH_CD<>T.ACCOUNTING_ATTACH_CD
 OR N.CUSTOMER_TYPE_CD<>T.CUSTOMER_TYPE_CD
 OR N.INDUSTRY_TYPE_CD<>T.INDUSTRY_TYPE_CD
 OR N.CUSTOMER_SIZE_CD<>T.CUSTOMER_SIZE_CD
 OR N.CALC_INTEREST_IND<>T.CALC_INTEREST_IND
 OR N.LOW_RISK_IND<>T.LOW_RISK_IND
 OR N.LIMIT_LOGIN_OCCASION_CD<>T.LIMIT_LOGIN_OCCASION_CD
 OR N.LIMIT_LOGIN_AMOUNT<>T.LIMIT_LOGIN_AMOUNT
 OR N.SUPERIOR_ID<>T.SUPERIOR_ID
 OR N.SUPERIOR_CD<>T.SUPERIOR_CD
 OR N.CHARGE_IND<>T.CHARGE_IND
 OR N.PRODUCT_KIND_CD<>T.PRODUCT_KIND_CD
 OR N.SPECIALTY<>T.SPECIALTY
 OR N.RSP_CODE<>T.RSP_CODE
 OR N.RSP_TIME<>T.RSP_TIME
 OR N.FXBS_TIME_MARK<>T.FXBS_TIME_MARK
 OR N.BUSINESS_MODE<>T.BUSINESS_MODE
 OR N.FINACING_MODE<>T.FINACING_MODE
;

--Step3:
UPDATE dw_sdata.CCS_004_TB_SYS_PRODUCT P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_124
WHERE P.End_Dt=DATE('2100-12-31')
AND P.PRODUCT_ID=T_124.PRODUCT_ID
;

--Step4:
INSERT  INTO dw_sdata.CCS_004_TB_SYS_PRODUCT SELECT * FROM T_124;

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
