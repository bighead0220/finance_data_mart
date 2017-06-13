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
DELETE FROM dw_sdata.PCS_006_TB_LIN_LINE WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_006_TB_LIN_LINE SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_332 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_006_TB_LIN_LINE WHERE 1=0;

--Step2:
INSERT  INTO T_332 (
  LINE_ID,
  LINE_NO,
  LINE_STATUS,
  CUS_ID,
  CUS_KIND,
  APP_OP_ID,
  RECORD_FLAG,
  MANAGER_USER_ID,
  REAL_MATURE_DATE,
  GROUP_LINE_ID,
  CURRENCY,
  LINE_AMOUNT,
  LINE_BALANCE,
  LINE_PERIOD,
  LINE_BEGIN_DATE,
  LINE_MATURE_DATE,
  LINE_AJUSTEND_DATE,
  LINE_EXPENSE_PERIOD,
  LOAN_AJUSTEND_DATE,
  LOAN_MAX_PERIOD,
  LINE_DURATION,
  ATM_LINE_SUPPPERIOD,
  LINE_TYPE,
  SECURITY_KIND,
  PROVINCE_NUM,
  VIRTUAL_FLAG,
  GROUP_ID,
  COM_SECURITY_KIND,
  CYCLE_FLAG,
  MARGIN_RATIO,
  MARGIN_WAY,
  MARGIN_FLAG,
  CREATE_TIME,
  UPDATE_TIME,
  DELFLAG,
  TRUNC_NO,
  GROUP_SECURITY_TYPE,
  IS_CARD_LOAN,
  ACCOUNT_NAME,
  ACCOUNT_KIND,
  ACCOUNT_NO,
  CYCLE_TYPE,
  BIZ_CHANNEL_KIND,
  BIZ_OPEN_CHANNELS,
  EBANK_CUS_NO,
  QUICK_LOAN_FLAG,
  IS_DISCOUNT,
  MARGIN_ACC,
  MARGIN_AMOUNT,
  start_dt,
  end_dt)
SELECT
  N.LINE_ID,
  N.LINE_NO,
  N.LINE_STATUS,
  N.CUS_ID,
  N.CUS_KIND,
  N.APP_OP_ID,
  N.RECORD_FLAG,
  N.MANAGER_USER_ID,
  N.REAL_MATURE_DATE,
  N.GROUP_LINE_ID,
  N.CURRENCY,
  N.LINE_AMOUNT,
  N.LINE_BALANCE,
  N.LINE_PERIOD,
  N.LINE_BEGIN_DATE,
  N.LINE_MATURE_DATE,
  N.LINE_AJUSTEND_DATE,
  N.LINE_EXPENSE_PERIOD,
  N.LOAN_AJUSTEND_DATE,
  N.LOAN_MAX_PERIOD,
  N.LINE_DURATION,
  N.ATM_LINE_SUPPPERIOD,
  N.LINE_TYPE,
  N.SECURITY_KIND,
  N.PROVINCE_NUM,
  N.VIRTUAL_FLAG,
  N.GROUP_ID,
  N.COM_SECURITY_KIND,
  N.CYCLE_FLAG,
  N.MARGIN_RATIO,
  N.MARGIN_WAY,
  N.MARGIN_FLAG,
  N.CREATE_TIME,
  N.UPDATE_TIME,
  N.DELFLAG,
  N.TRUNC_NO,
  N.GROUP_SECURITY_TYPE,
  N.IS_CARD_LOAN,
  N.ACCOUNT_NAME,
  N.ACCOUNT_KIND,
  N.ACCOUNT_NO,
  N.CYCLE_TYPE,
  N.BIZ_CHANNEL_KIND,
  N.BIZ_OPEN_CHANNELS,
  N.EBANK_CUS_NO,
  N.QUICK_LOAN_FLAG,
  N.IS_DISCOUNT,
  N.MARGIN_ACC,
  N.MARGIN_AMOUNT,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(LINE_ID, '' ) AS LINE_ID ,
  COALESCE(LINE_NO, '' ) AS LINE_NO ,
  COALESCE(LINE_STATUS, '' ) AS LINE_STATUS ,
  COALESCE(CUS_ID, '' ) AS CUS_ID ,
  COALESCE(CUS_KIND, '' ) AS CUS_KIND ,
  COALESCE(APP_OP_ID, '' ) AS APP_OP_ID ,
  COALESCE(RECORD_FLAG, '' ) AS RECORD_FLAG ,
  COALESCE(MANAGER_USER_ID, '' ) AS MANAGER_USER_ID ,
  COALESCE(REAL_MATURE_DATE,DATE('4999-12-31') ) AS REAL_MATURE_DATE ,
  COALESCE(GROUP_LINE_ID, '' ) AS GROUP_LINE_ID ,
  COALESCE(CURRENCY, '' ) AS CURRENCY ,
  COALESCE(LINE_AMOUNT, 0 ) AS LINE_AMOUNT ,
  COALESCE(LINE_BALANCE, 0 ) AS LINE_BALANCE ,
  COALESCE(LINE_PERIOD, '' ) AS LINE_PERIOD ,
  COALESCE(LINE_BEGIN_DATE,DATE('4999-12-31') ) AS LINE_BEGIN_DATE ,
  COALESCE(LINE_MATURE_DATE,DATE('4999-12-31') ) AS LINE_MATURE_DATE ,
  COALESCE(LINE_AJUSTEND_DATE,DATE('4999-12-31') ) AS LINE_AJUSTEND_DATE ,
  COALESCE(LINE_EXPENSE_PERIOD, '' ) AS LINE_EXPENSE_PERIOD ,
  COALESCE(LOAN_AJUSTEND_DATE,DATE('4999-12-31') ) AS LOAN_AJUSTEND_DATE ,
  COALESCE(LOAN_MAX_PERIOD, '' ) AS LOAN_MAX_PERIOD ,
  COALESCE(LINE_DURATION, '' ) AS LINE_DURATION ,
  COALESCE(ATM_LINE_SUPPPERIOD, '' ) AS ATM_LINE_SUPPPERIOD ,
  COALESCE(LINE_TYPE, '' ) AS LINE_TYPE ,
  COALESCE(SECURITY_KIND, '' ) AS SECURITY_KIND ,
  COALESCE(PROVINCE_NUM, '' ) AS PROVINCE_NUM ,
  COALESCE(VIRTUAL_FLAG, '' ) AS VIRTUAL_FLAG ,
  COALESCE(GROUP_ID, '' ) AS GROUP_ID ,
  COALESCE(COM_SECURITY_KIND, '' ) AS COM_SECURITY_KIND ,
  COALESCE(CYCLE_FLAG, '' ) AS CYCLE_FLAG ,
  COALESCE(MARGIN_RATIO, 0 ) AS MARGIN_RATIO ,
  COALESCE(MARGIN_WAY, '' ) AS MARGIN_WAY ,
  COALESCE(MARGIN_FLAG, '' ) AS MARGIN_FLAG ,
  COALESCE(CREATE_TIME,'4999-12-31 00:00:00' ) AS CREATE_TIME ,
  COALESCE(UPDATE_TIME,'4999-12-31 00:00:00' ) AS UPDATE_TIME ,
  COALESCE(DELFLAG, '' ) AS DELFLAG ,
  COALESCE(TRUNC_NO, 0 ) AS TRUNC_NO ,
  COALESCE(GROUP_SECURITY_TYPE, '' ) AS GROUP_SECURITY_TYPE ,
  COALESCE(IS_CARD_LOAN, '' ) AS IS_CARD_LOAN ,
  COALESCE(ACCOUNT_NAME, '' ) AS ACCOUNT_NAME ,
  COALESCE(ACCOUNT_KIND, '' ) AS ACCOUNT_KIND ,
  COALESCE(ACCOUNT_NO, '' ) AS ACCOUNT_NO ,
  COALESCE(CYCLE_TYPE, '' ) AS CYCLE_TYPE ,
  COALESCE(BIZ_CHANNEL_KIND, '' ) AS BIZ_CHANNEL_KIND ,
  COALESCE(BIZ_OPEN_CHANNELS, '' ) AS BIZ_OPEN_CHANNELS ,
  COALESCE(EBANK_CUS_NO, '' ) AS EBANK_CUS_NO ,
  COALESCE(QUICK_LOAN_FLAG, '' ) AS QUICK_LOAN_FLAG ,
  COALESCE(IS_DISCOUNT, '' ) AS IS_DISCOUNT ,
  COALESCE(MARGIN_ACC, '' ) AS MARGIN_ACC ,
  COALESCE(MARGIN_AMOUNT, 0 ) AS MARGIN_AMOUNT 
 FROM  dw_tdata.PCS_006_TB_LIN_LINE_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  LINE_ID ,
  LINE_NO ,
  LINE_STATUS ,
  CUS_ID ,
  CUS_KIND ,
  APP_OP_ID ,
  RECORD_FLAG ,
  MANAGER_USER_ID ,
  REAL_MATURE_DATE ,
  GROUP_LINE_ID ,
  CURRENCY ,
  LINE_AMOUNT ,
  LINE_BALANCE ,
  LINE_PERIOD ,
  LINE_BEGIN_DATE ,
  LINE_MATURE_DATE ,
  LINE_AJUSTEND_DATE ,
  LINE_EXPENSE_PERIOD ,
  LOAN_AJUSTEND_DATE ,
  LOAN_MAX_PERIOD ,
  LINE_DURATION ,
  ATM_LINE_SUPPPERIOD ,
  LINE_TYPE ,
  SECURITY_KIND ,
  PROVINCE_NUM ,
  VIRTUAL_FLAG ,
  GROUP_ID ,
  COM_SECURITY_KIND ,
  CYCLE_FLAG ,
  MARGIN_RATIO ,
  MARGIN_WAY ,
  MARGIN_FLAG ,
  CREATE_TIME ,
  UPDATE_TIME ,
  DELFLAG ,
  TRUNC_NO ,
  GROUP_SECURITY_TYPE ,
  IS_CARD_LOAN ,
  ACCOUNT_NAME ,
  ACCOUNT_KIND ,
  ACCOUNT_NO ,
  CYCLE_TYPE ,
  BIZ_CHANNEL_KIND ,
  BIZ_OPEN_CHANNELS ,
  EBANK_CUS_NO ,
  QUICK_LOAN_FLAG ,
  IS_DISCOUNT ,
  MARGIN_ACC ,
  MARGIN_AMOUNT 
 FROM dw_sdata.PCS_006_TB_LIN_LINE 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.LINE_ID = T.LINE_ID
WHERE
(T.LINE_ID IS NULL)
 OR N.LINE_NO<>T.LINE_NO
 OR N.LINE_STATUS<>T.LINE_STATUS
 OR N.CUS_ID<>T.CUS_ID
 OR N.CUS_KIND<>T.CUS_KIND
 OR N.APP_OP_ID<>T.APP_OP_ID
 OR N.RECORD_FLAG<>T.RECORD_FLAG
 OR N.MANAGER_USER_ID<>T.MANAGER_USER_ID
 OR N.REAL_MATURE_DATE<>T.REAL_MATURE_DATE
 OR N.GROUP_LINE_ID<>T.GROUP_LINE_ID
 OR N.CURRENCY<>T.CURRENCY
 OR N.LINE_AMOUNT<>T.LINE_AMOUNT
 OR N.LINE_BALANCE<>T.LINE_BALANCE
 OR N.LINE_PERIOD<>T.LINE_PERIOD
 OR N.LINE_BEGIN_DATE<>T.LINE_BEGIN_DATE
 OR N.LINE_MATURE_DATE<>T.LINE_MATURE_DATE
 OR N.LINE_AJUSTEND_DATE<>T.LINE_AJUSTEND_DATE
 OR N.LINE_EXPENSE_PERIOD<>T.LINE_EXPENSE_PERIOD
 OR N.LOAN_AJUSTEND_DATE<>T.LOAN_AJUSTEND_DATE
 OR N.LOAN_MAX_PERIOD<>T.LOAN_MAX_PERIOD
 OR N.LINE_DURATION<>T.LINE_DURATION
 OR N.ATM_LINE_SUPPPERIOD<>T.ATM_LINE_SUPPPERIOD
 OR N.LINE_TYPE<>T.LINE_TYPE
 OR N.SECURITY_KIND<>T.SECURITY_KIND
 OR N.PROVINCE_NUM<>T.PROVINCE_NUM
 OR N.VIRTUAL_FLAG<>T.VIRTUAL_FLAG
 OR N.GROUP_ID<>T.GROUP_ID
 OR N.COM_SECURITY_KIND<>T.COM_SECURITY_KIND
 OR N.CYCLE_FLAG<>T.CYCLE_FLAG
 OR N.MARGIN_RATIO<>T.MARGIN_RATIO
 OR N.MARGIN_WAY<>T.MARGIN_WAY
 OR N.MARGIN_FLAG<>T.MARGIN_FLAG
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.UPDATE_TIME<>T.UPDATE_TIME
 OR N.DELFLAG<>T.DELFLAG
 OR N.TRUNC_NO<>T.TRUNC_NO
 OR N.GROUP_SECURITY_TYPE<>T.GROUP_SECURITY_TYPE
 OR N.IS_CARD_LOAN<>T.IS_CARD_LOAN
 OR N.ACCOUNT_NAME<>T.ACCOUNT_NAME
 OR N.ACCOUNT_KIND<>T.ACCOUNT_KIND
 OR N.ACCOUNT_NO<>T.ACCOUNT_NO
 OR N.CYCLE_TYPE<>T.CYCLE_TYPE
 OR N.BIZ_CHANNEL_KIND<>T.BIZ_CHANNEL_KIND
 OR N.BIZ_OPEN_CHANNELS<>T.BIZ_OPEN_CHANNELS
 OR N.EBANK_CUS_NO<>T.EBANK_CUS_NO
 OR N.QUICK_LOAN_FLAG<>T.QUICK_LOAN_FLAG
 OR N.IS_DISCOUNT<>T.IS_DISCOUNT
 OR N.MARGIN_ACC<>T.MARGIN_ACC
 OR N.MARGIN_AMOUNT<>T.MARGIN_AMOUNT
;

--Step3:
UPDATE dw_sdata.PCS_006_TB_LIN_LINE P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_332
WHERE P.End_Dt=DATE('2100-12-31')
AND P.LINE_ID=T_332.LINE_ID
;

--Step4:
INSERT  INTO dw_sdata.PCS_006_TB_LIN_LINE SELECT * FROM T_332;

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