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
DELETE FROM dw_sdata.FSS_001_GF_PROD_BASE_INFO WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.FSS_001_GF_PROD_BASE_INFO SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_361 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.FSS_001_GF_PROD_BASE_INFO WHERE 1=0;

--Step2:
INSERT  INTO T_361 (
  PROD_CODE,
  PROD_NAME,
  PROD_NAME_SUFFIX,
  PROFIT_TYPE,
  PROD_OPER_MODEL,
  PROD_RISK_LEVEL,
  IS_CONT_PROD,
  IS_NEED_CONT_PROD,
  IS_AUTO_BALANCE,
  CURRENCY,
  ACCOUNTING_FLAG,
  IS_NIGHT_MARKET,
  FEE_PROV_FLAG,
  IS_QUANTO,
  PROD_BEGIN_DATE,
  PROD_END_DATE,
  PROD_DURATION_DAYS,
  MOUTH_PROFIT_DATE,
  PROD_STATUS,
  CUST_TYPE,
  IS_WHITE_LIST,
  IS_POINT,
  IS_POS,
  IS_CUST_PROP,
  VIP_GRADE,
  HIGHEST_INVESTMENT,
  LOWEST_INVESTMENT,
  MULTI_MONEY,
  HOLD_MONEY,
  CORP_HIGHEST_INVESTMENT,
  CORP_LOWEST_INVESTMENT,
  CORP_MULTI_MONEY,
  CORP_HOLD_MONEY,
  DIVIDEND_FLAG,
  DEFAULT_DIVIDEND_FLAG,
  IS_DIVIDEND_ALTER,
  IS_ASSET_PROVE,
  IS_ORDER_AUTO_BUY,
  IS_DURATION,
  IS_DATE_INVEST,
  TRANSITION_FLAG,
  IS_PLEDGE,
  PROFIT_HANDLE_FLAG,
  SELL_FEE_TYPE,
  PROD_CONTROL_FEE_RATE,
  PREDICT_HIGHEST_PROFIT,
  PREDICT_LOWEST_PROFIT,
  OVERFULFIL_PROFIT,
  PROD_PROFIT_RATE,
  PROD_RECOM_RATE,
  OTHER_RATE,
  DAY_COUNT_BASIS,
  HEAD_OFFICE_MAG_FEE,
  FEL_MAG_FEE,
  NONRECURRING_CHARGE,
  PROD_OPER_CODE,
  LATEST_MODIFY_DATE,
  LATEST_MODIFY_TIME,
  LATEST_OPER_CODE,
  start_dt,
  end_dt)
SELECT
  N.PROD_CODE,
  N.PROD_NAME,
  N.PROD_NAME_SUFFIX,
  N.PROFIT_TYPE,
  N.PROD_OPER_MODEL,
  N.PROD_RISK_LEVEL,
  N.IS_CONT_PROD,
  N.IS_NEED_CONT_PROD,
  N.IS_AUTO_BALANCE,
  N.CURRENCY,
  N.ACCOUNTING_FLAG,
  N.IS_NIGHT_MARKET,
  N.FEE_PROV_FLAG,
  N.IS_QUANTO,
  N.PROD_BEGIN_DATE,
  N.PROD_END_DATE,
  N.PROD_DURATION_DAYS,
  N.MOUTH_PROFIT_DATE,
  N.PROD_STATUS,
  N.CUST_TYPE,
  N.IS_WHITE_LIST,
  N.IS_POINT,
  N.IS_POS,
  N.IS_CUST_PROP,
  N.VIP_GRADE,
  N.HIGHEST_INVESTMENT,
  N.LOWEST_INVESTMENT,
  N.MULTI_MONEY,
  N.HOLD_MONEY,
  N.CORP_HIGHEST_INVESTMENT,
  N.CORP_LOWEST_INVESTMENT,
  N.CORP_MULTI_MONEY,
  N.CORP_HOLD_MONEY,
  N.DIVIDEND_FLAG,
  N.DEFAULT_DIVIDEND_FLAG,
  N.IS_DIVIDEND_ALTER,
  N.IS_ASSET_PROVE,
  N.IS_ORDER_AUTO_BUY,
  N.IS_DURATION,
  N.IS_DATE_INVEST,
  N.TRANSITION_FLAG,
  N.IS_PLEDGE,
  N.PROFIT_HANDLE_FLAG,
  N.SELL_FEE_TYPE,
  N.PROD_CONTROL_FEE_RATE,
  N.PREDICT_HIGHEST_PROFIT,
  N.PREDICT_LOWEST_PROFIT,
  N.OVERFULFIL_PROFIT,
  N.PROD_PROFIT_RATE,
  N.PROD_RECOM_RATE,
  N.OTHER_RATE,
  N.DAY_COUNT_BASIS,
  N.HEAD_OFFICE_MAG_FEE,
  N.FEL_MAG_FEE,
  N.NONRECURRING_CHARGE,
  N.PROD_OPER_CODE,
  N.LATEST_MODIFY_DATE,
  N.LATEST_MODIFY_TIME,
  N.LATEST_OPER_CODE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(PROD_CODE, '' ) AS PROD_CODE ,
  COALESCE(PROD_NAME, '' ) AS PROD_NAME ,
  COALESCE(PROD_NAME_SUFFIX, '' ) AS PROD_NAME_SUFFIX ,
  COALESCE(PROFIT_TYPE, '' ) AS PROFIT_TYPE ,
  COALESCE(PROD_OPER_MODEL, '' ) AS PROD_OPER_MODEL ,
  COALESCE(PROD_RISK_LEVEL, '' ) AS PROD_RISK_LEVEL ,
  COALESCE(IS_CONT_PROD, '' ) AS IS_CONT_PROD ,
  COALESCE(IS_NEED_CONT_PROD, '' ) AS IS_NEED_CONT_PROD ,
  COALESCE(IS_AUTO_BALANCE, '' ) AS IS_AUTO_BALANCE ,
  COALESCE(CURRENCY, '' ) AS CURRENCY ,
  COALESCE(ACCOUNTING_FLAG, '' ) AS ACCOUNTING_FLAG ,
  COALESCE(IS_NIGHT_MARKET, '' ) AS IS_NIGHT_MARKET ,
  COALESCE(FEE_PROV_FLAG, '' ) AS FEE_PROV_FLAG ,
  COALESCE(IS_QUANTO, '' ) AS IS_QUANTO ,
  COALESCE(PROD_BEGIN_DATE, '' ) AS PROD_BEGIN_DATE ,
  COALESCE(PROD_END_DATE, '' ) AS PROD_END_DATE ,
  COALESCE(PROD_DURATION_DAYS, '' ) AS PROD_DURATION_DAYS ,
  COALESCE(MOUTH_PROFIT_DATE, 0 ) AS MOUTH_PROFIT_DATE ,
  COALESCE(PROD_STATUS, '' ) AS PROD_STATUS ,
  COALESCE(CUST_TYPE, '' ) AS CUST_TYPE ,
  COALESCE(IS_WHITE_LIST, '' ) AS IS_WHITE_LIST ,
  COALESCE(IS_POINT, '' ) AS IS_POINT ,
  COALESCE(IS_POS, '' ) AS IS_POS ,
  COALESCE(IS_CUST_PROP, '' ) AS IS_CUST_PROP ,
  COALESCE(VIP_GRADE, '' ) AS VIP_GRADE ,
  COALESCE(HIGHEST_INVESTMENT, 0 ) AS HIGHEST_INVESTMENT ,
  COALESCE(LOWEST_INVESTMENT, 0 ) AS LOWEST_INVESTMENT ,
  COALESCE(MULTI_MONEY, 0 ) AS MULTI_MONEY ,
  COALESCE(HOLD_MONEY, 0 ) AS HOLD_MONEY ,
  COALESCE(CORP_HIGHEST_INVESTMENT, 0 ) AS CORP_HIGHEST_INVESTMENT ,
  COALESCE(CORP_LOWEST_INVESTMENT, 0 ) AS CORP_LOWEST_INVESTMENT ,
  COALESCE(CORP_MULTI_MONEY, 0 ) AS CORP_MULTI_MONEY ,
  COALESCE(CORP_HOLD_MONEY, 0 ) AS CORP_HOLD_MONEY ,
  COALESCE(DIVIDEND_FLAG, '' ) AS DIVIDEND_FLAG ,
  COALESCE(DEFAULT_DIVIDEND_FLAG, '' ) AS DEFAULT_DIVIDEND_FLAG ,
  COALESCE(IS_DIVIDEND_ALTER, '' ) AS IS_DIVIDEND_ALTER ,
  COALESCE(IS_ASSET_PROVE, '' ) AS IS_ASSET_PROVE ,
  COALESCE(IS_ORDER_AUTO_BUY, '' ) AS IS_ORDER_AUTO_BUY ,
  COALESCE(IS_DURATION, '' ) AS IS_DURATION ,
  COALESCE(IS_DATE_INVEST, '' ) AS IS_DATE_INVEST ,
  COALESCE(TRANSITION_FLAG, '' ) AS TRANSITION_FLAG ,
  COALESCE(IS_PLEDGE, '' ) AS IS_PLEDGE ,
  COALESCE(PROFIT_HANDLE_FLAG, '' ) AS PROFIT_HANDLE_FLAG ,
  COALESCE(SELL_FEE_TYPE, '' ) AS SELL_FEE_TYPE ,
  COALESCE(PROD_CONTROL_FEE_RATE, 0 ) AS PROD_CONTROL_FEE_RATE ,
  COALESCE(PREDICT_HIGHEST_PROFIT, 0 ) AS PREDICT_HIGHEST_PROFIT ,
  COALESCE(PREDICT_LOWEST_PROFIT, 0 ) AS PREDICT_LOWEST_PROFIT ,
  COALESCE(OVERFULFIL_PROFIT, 0 ) AS OVERFULFIL_PROFIT ,
  COALESCE(PROD_PROFIT_RATE, 0 ) AS PROD_PROFIT_RATE ,
  COALESCE(PROD_RECOM_RATE, 0 ) AS PROD_RECOM_RATE ,
  COALESCE(OTHER_RATE, 0 ) AS OTHER_RATE ,
  COALESCE(DAY_COUNT_BASIS, '' ) AS DAY_COUNT_BASIS ,
  COALESCE(HEAD_OFFICE_MAG_FEE, 0 ) AS HEAD_OFFICE_MAG_FEE ,
  COALESCE(FEL_MAG_FEE, 0 ) AS FEL_MAG_FEE ,
  COALESCE(NONRECURRING_CHARGE, 0 ) AS NONRECURRING_CHARGE ,
  COALESCE(PROD_OPER_CODE, '' ) AS PROD_OPER_CODE ,
  COALESCE(LATEST_MODIFY_DATE, '' ) AS LATEST_MODIFY_DATE ,
  COALESCE(LATEST_MODIFY_TIME, '' ) AS LATEST_MODIFY_TIME ,
  COALESCE(LATEST_OPER_CODE, '' ) AS LATEST_OPER_CODE 
 FROM  dw_tdata.FSS_001_GF_PROD_BASE_INFO_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  PROD_CODE ,
  PROD_NAME ,
  PROD_NAME_SUFFIX ,
  PROFIT_TYPE ,
  PROD_OPER_MODEL ,
  PROD_RISK_LEVEL ,
  IS_CONT_PROD ,
  IS_NEED_CONT_PROD ,
  IS_AUTO_BALANCE ,
  CURRENCY ,
  ACCOUNTING_FLAG ,
  IS_NIGHT_MARKET ,
  FEE_PROV_FLAG ,
  IS_QUANTO ,
  PROD_BEGIN_DATE ,
  PROD_END_DATE ,
  PROD_DURATION_DAYS ,
  MOUTH_PROFIT_DATE ,
  PROD_STATUS ,
  CUST_TYPE ,
  IS_WHITE_LIST ,
  IS_POINT ,
  IS_POS ,
  IS_CUST_PROP ,
  VIP_GRADE ,
  HIGHEST_INVESTMENT ,
  LOWEST_INVESTMENT ,
  MULTI_MONEY ,
  HOLD_MONEY ,
  CORP_HIGHEST_INVESTMENT ,
  CORP_LOWEST_INVESTMENT ,
  CORP_MULTI_MONEY ,
  CORP_HOLD_MONEY ,
  DIVIDEND_FLAG ,
  DEFAULT_DIVIDEND_FLAG ,
  IS_DIVIDEND_ALTER ,
  IS_ASSET_PROVE ,
  IS_ORDER_AUTO_BUY ,
  IS_DURATION ,
  IS_DATE_INVEST ,
  TRANSITION_FLAG ,
  IS_PLEDGE ,
  PROFIT_HANDLE_FLAG ,
  SELL_FEE_TYPE ,
  PROD_CONTROL_FEE_RATE ,
  PREDICT_HIGHEST_PROFIT ,
  PREDICT_LOWEST_PROFIT ,
  OVERFULFIL_PROFIT ,
  PROD_PROFIT_RATE ,
  PROD_RECOM_RATE ,
  OTHER_RATE ,
  DAY_COUNT_BASIS ,
  HEAD_OFFICE_MAG_FEE ,
  FEL_MAG_FEE ,
  NONRECURRING_CHARGE ,
  PROD_OPER_CODE ,
  LATEST_MODIFY_DATE ,
  LATEST_MODIFY_TIME ,
  LATEST_OPER_CODE 
 FROM dw_sdata.FSS_001_GF_PROD_BASE_INFO 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.PROD_CODE = T.PROD_CODE
WHERE
(T.PROD_CODE IS NULL)
 OR N.PROD_NAME<>T.PROD_NAME
 OR N.PROD_NAME_SUFFIX<>T.PROD_NAME_SUFFIX
 OR N.PROFIT_TYPE<>T.PROFIT_TYPE
 OR N.PROD_OPER_MODEL<>T.PROD_OPER_MODEL
 OR N.PROD_RISK_LEVEL<>T.PROD_RISK_LEVEL
 OR N.IS_CONT_PROD<>T.IS_CONT_PROD
 OR N.IS_NEED_CONT_PROD<>T.IS_NEED_CONT_PROD
 OR N.IS_AUTO_BALANCE<>T.IS_AUTO_BALANCE
 OR N.CURRENCY<>T.CURRENCY
 OR N.ACCOUNTING_FLAG<>T.ACCOUNTING_FLAG
 OR N.IS_NIGHT_MARKET<>T.IS_NIGHT_MARKET
 OR N.FEE_PROV_FLAG<>T.FEE_PROV_FLAG
 OR N.IS_QUANTO<>T.IS_QUANTO
 OR N.PROD_BEGIN_DATE<>T.PROD_BEGIN_DATE
 OR N.PROD_END_DATE<>T.PROD_END_DATE
 OR N.PROD_DURATION_DAYS<>T.PROD_DURATION_DAYS
 OR N.MOUTH_PROFIT_DATE<>T.MOUTH_PROFIT_DATE
 OR N.PROD_STATUS<>T.PROD_STATUS
 OR N.CUST_TYPE<>T.CUST_TYPE
 OR N.IS_WHITE_LIST<>T.IS_WHITE_LIST
 OR N.IS_POINT<>T.IS_POINT
 OR N.IS_POS<>T.IS_POS
 OR N.IS_CUST_PROP<>T.IS_CUST_PROP
 OR N.VIP_GRADE<>T.VIP_GRADE
 OR N.HIGHEST_INVESTMENT<>T.HIGHEST_INVESTMENT
 OR N.LOWEST_INVESTMENT<>T.LOWEST_INVESTMENT
 OR N.MULTI_MONEY<>T.MULTI_MONEY
 OR N.HOLD_MONEY<>T.HOLD_MONEY
 OR N.CORP_HIGHEST_INVESTMENT<>T.CORP_HIGHEST_INVESTMENT
 OR N.CORP_LOWEST_INVESTMENT<>T.CORP_LOWEST_INVESTMENT
 OR N.CORP_MULTI_MONEY<>T.CORP_MULTI_MONEY
 OR N.CORP_HOLD_MONEY<>T.CORP_HOLD_MONEY
 OR N.DIVIDEND_FLAG<>T.DIVIDEND_FLAG
 OR N.DEFAULT_DIVIDEND_FLAG<>T.DEFAULT_DIVIDEND_FLAG
 OR N.IS_DIVIDEND_ALTER<>T.IS_DIVIDEND_ALTER
 OR N.IS_ASSET_PROVE<>T.IS_ASSET_PROVE
 OR N.IS_ORDER_AUTO_BUY<>T.IS_ORDER_AUTO_BUY
 OR N.IS_DURATION<>T.IS_DURATION
 OR N.IS_DATE_INVEST<>T.IS_DATE_INVEST
 OR N.TRANSITION_FLAG<>T.TRANSITION_FLAG
 OR N.IS_PLEDGE<>T.IS_PLEDGE
 OR N.PROFIT_HANDLE_FLAG<>T.PROFIT_HANDLE_FLAG
 OR N.SELL_FEE_TYPE<>T.SELL_FEE_TYPE
 OR N.PROD_CONTROL_FEE_RATE<>T.PROD_CONTROL_FEE_RATE
 OR N.PREDICT_HIGHEST_PROFIT<>T.PREDICT_HIGHEST_PROFIT
 OR N.PREDICT_LOWEST_PROFIT<>T.PREDICT_LOWEST_PROFIT
 OR N.OVERFULFIL_PROFIT<>T.OVERFULFIL_PROFIT
 OR N.PROD_PROFIT_RATE<>T.PROD_PROFIT_RATE
 OR N.PROD_RECOM_RATE<>T.PROD_RECOM_RATE
 OR N.OTHER_RATE<>T.OTHER_RATE
 OR N.DAY_COUNT_BASIS<>T.DAY_COUNT_BASIS
 OR N.HEAD_OFFICE_MAG_FEE<>T.HEAD_OFFICE_MAG_FEE
 OR N.FEL_MAG_FEE<>T.FEL_MAG_FEE
 OR N.NONRECURRING_CHARGE<>T.NONRECURRING_CHARGE
 OR N.PROD_OPER_CODE<>T.PROD_OPER_CODE
 OR N.LATEST_MODIFY_DATE<>T.LATEST_MODIFY_DATE
 OR N.LATEST_MODIFY_TIME<>T.LATEST_MODIFY_TIME
 OR N.LATEST_OPER_CODE<>T.LATEST_OPER_CODE
;

--Step3:
UPDATE dw_sdata.FSS_001_GF_PROD_BASE_INFO P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_361
WHERE P.End_Dt=DATE('2100-12-31')
AND P.PROD_CODE=T_361.PROD_CODE
;

--Step4:
INSERT  INTO dw_sdata.FSS_001_GF_PROD_BASE_INFO SELECT * FROM T_361;

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
