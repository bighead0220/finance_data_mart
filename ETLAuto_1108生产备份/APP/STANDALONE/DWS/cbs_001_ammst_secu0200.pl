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
DELETE FROM dw_sdata.CBS_001_AMMST_SECU WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CBS_001_AMMST_SECU SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_71 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CBS_001_AMMST_SECU WHERE 1=0;

--Step2:
INSERT  INTO T_71 (
  INTER_ACCT,
  SUBACCT,
  ACCOUNT,
  CUR_CODE,
  CUST_ID,
  FUND_TYPE,
  ACCT_NAME,
  ACCT_ENAME,
  OPEN_UNIT,
  ACCT_KIND,
  CASH_REMIT_FLAG,
  SUBACCT_SYS,
  SECU_PURPOSE,
  DEP_TYPE,
  ACCT_CHARA,
  YDAY_BAL,
  CUR_BAL,
  BAL_FLAG,
  DRAW_FLAG,
  SEAL_CARD_NO,
  OPEN_CONF_NO,
  PAY_PSSWD_ORG,
  CHECK_FLAG,
  ACCT_STATE,
  LOSS_FLAG,
  FROZ_FLAG,
  FROZ_AMT,
  STOP_AMT,
  CERT_AMT,
  IMPAWN_AMT,
  OPEN_DATE,
  INT_START_DATE,
  CHECK_DATE,
  DEP_TERM,
  CLOSE_DATE,
  DUE_DATE,
  BRK_DEP_NUM,
  LAST_CI_DATE,
  LST_CLR_DATE,
  LST_OCC_DATE,
  TIME_STAMP,
  I_FLAG,
  I_MTHD,
  IRATE_KIND,
  IRATE_LEVEL,
  IRATE_FLOAT_TYPE,
  FLOAT_IRATE,
  FLOAT_POINTS,
  IRATE_CODE,
  IRATE,
  IRATE_ADJ_DATE,
  IRATE_ADJUST_WAY,
  ADJ_CYCLE_TYPE,
  ADJ_BEGIN_DATE,
  SPECIFIED_DATE,
  ADJ_IWEGH,
  ROOT_ETR_ACCT,
  SETTL_ACCT,
  VCHR_TYPE,
  VCHR_NO,
  PRTOVR_PAGERS,
  NO_PRT_CONT,
  AUTO_FLAG,
  AUTO_TIMES,
  HAVE_TIMES,
  APPOINT_DRAW_FLAG,
  APPOINT_ACCT,
  CUST_MGR_TYPE,
  CUST_MGR_NO,
  CUST_MGR_NAME,
  DEPTERM_FLAG,
  DAC,
  start_dt,
  end_dt)
SELECT
  N.INTER_ACCT,
  N.SUBACCT,
  N.ACCOUNT,
  N.CUR_CODE,
  N.CUST_ID,
  N.FUND_TYPE,
  N.ACCT_NAME,
  N.ACCT_ENAME,
  N.OPEN_UNIT,
  N.ACCT_KIND,
  N.CASH_REMIT_FLAG,
  N.SUBACCT_SYS,
  N.SECU_PURPOSE,
  N.DEP_TYPE,
  N.ACCT_CHARA,
  N.YDAY_BAL,
  N.CUR_BAL,
  N.BAL_FLAG,
  N.DRAW_FLAG,
  N.SEAL_CARD_NO,
  N.OPEN_CONF_NO,
  N.PAY_PSSWD_ORG,
  N.CHECK_FLAG,
  N.ACCT_STATE,
  N.LOSS_FLAG,
  N.FROZ_FLAG,
  N.FROZ_AMT,
  N.STOP_AMT,
  N.CERT_AMT,
  N.IMPAWN_AMT,
  N.OPEN_DATE,
  N.INT_START_DATE,
  N.CHECK_DATE,
  N.DEP_TERM,
  N.CLOSE_DATE,
  N.DUE_DATE,
  N.BRK_DEP_NUM,
  N.LAST_CI_DATE,
  N.LST_CLR_DATE,
  N.LST_OCC_DATE,
  N.TIME_STAMP,
  N.I_FLAG,
  N.I_MTHD,
  N.IRATE_KIND,
  N.IRATE_LEVEL,
  N.IRATE_FLOAT_TYPE,
  N.FLOAT_IRATE,
  N.FLOAT_POINTS,
  N.IRATE_CODE,
  N.IRATE,
  N.IRATE_ADJ_DATE,
  N.IRATE_ADJUST_WAY,
  N.ADJ_CYCLE_TYPE,
  N.ADJ_BEGIN_DATE,
  N.SPECIFIED_DATE,
  N.ADJ_IWEGH,
  N.ROOT_ETR_ACCT,
  N.SETTL_ACCT,
  N.VCHR_TYPE,
  N.VCHR_NO,
  N.PRTOVR_PAGERS,
  N.NO_PRT_CONT,
  N.AUTO_FLAG,
  N.AUTO_TIMES,
  N.HAVE_TIMES,
  N.APPOINT_DRAW_FLAG,
  N.APPOINT_ACCT,
  N.CUST_MGR_TYPE,
  N.CUST_MGR_NO,
  N.CUST_MGR_NAME,
  N.DEPTERM_FLAG,
  N.DAC,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(INTER_ACCT, '' ) AS INTER_ACCT ,
  COALESCE(SUBACCT, 0 ) AS SUBACCT ,
  COALESCE(ACCOUNT, '' ) AS ACCOUNT ,
  COALESCE(CUR_CODE, '' ) AS CUR_CODE ,
  COALESCE(CUST_ID, '' ) AS CUST_ID ,
  COALESCE(FUND_TYPE, '' ) AS FUND_TYPE ,
  COALESCE(ACCT_NAME, '' ) AS ACCT_NAME ,
  COALESCE(ACCT_ENAME, '' ) AS ACCT_ENAME ,
  COALESCE(OPEN_UNIT, '' ) AS OPEN_UNIT ,
  COALESCE(ACCT_KIND, '' ) AS ACCT_KIND ,
  COALESCE(CASH_REMIT_FLAG, '' ) AS CASH_REMIT_FLAG ,
  COALESCE(SUBACCT_SYS, '' ) AS SUBACCT_SYS ,
  COALESCE(SECU_PURPOSE, '' ) AS SECU_PURPOSE ,
  COALESCE(DEP_TYPE, '' ) AS DEP_TYPE ,
  COALESCE(ACCT_CHARA, '' ) AS ACCT_CHARA ,
  COALESCE(YDAY_BAL, 0 ) AS YDAY_BAL ,
  COALESCE(CUR_BAL, 0 ) AS CUR_BAL ,
  COALESCE(BAL_FLAG, '' ) AS BAL_FLAG ,
  COALESCE(DRAW_FLAG, '' ) AS DRAW_FLAG ,
  COALESCE(SEAL_CARD_NO, '' ) AS SEAL_CARD_NO ,
  COALESCE(OPEN_CONF_NO, '' ) AS OPEN_CONF_NO ,
  COALESCE(PAY_PSSWD_ORG, '' ) AS PAY_PSSWD_ORG ,
  COALESCE(CHECK_FLAG, '' ) AS CHECK_FLAG ,
  COALESCE(ACCT_STATE, '' ) AS ACCT_STATE ,
  COALESCE(LOSS_FLAG, '' ) AS LOSS_FLAG ,
  COALESCE(FROZ_FLAG, '' ) AS FROZ_FLAG ,
  COALESCE(FROZ_AMT, 0 ) AS FROZ_AMT ,
  COALESCE(STOP_AMT, 0 ) AS STOP_AMT ,
  COALESCE(CERT_AMT, 0 ) AS CERT_AMT ,
  COALESCE(IMPAWN_AMT, 0 ) AS IMPAWN_AMT ,
  COALESCE(OPEN_DATE, '' ) AS OPEN_DATE ,
  COALESCE(INT_START_DATE, '' ) AS INT_START_DATE ,
  COALESCE(CHECK_DATE, '' ) AS CHECK_DATE ,
  COALESCE(DEP_TERM, 0 ) AS DEP_TERM ,
  COALESCE(CLOSE_DATE, '' ) AS CLOSE_DATE ,
  COALESCE(DUE_DATE, '' ) AS DUE_DATE ,
  COALESCE(BRK_DEP_NUM, 0 ) AS BRK_DEP_NUM ,
  COALESCE(LAST_CI_DATE, '' ) AS LAST_CI_DATE ,
  COALESCE(LST_CLR_DATE, '' ) AS LST_CLR_DATE ,
  COALESCE(LST_OCC_DATE, '' ) AS LST_OCC_DATE ,
  COALESCE(TIME_STAMP, '' ) AS TIME_STAMP ,
  COALESCE(I_FLAG, '' ) AS I_FLAG ,
  COALESCE(I_MTHD, '' ) AS I_MTHD ,
  COALESCE(IRATE_KIND, '' ) AS IRATE_KIND ,
  COALESCE(IRATE_LEVEL, '' ) AS IRATE_LEVEL ,
  COALESCE(IRATE_FLOAT_TYPE, '' ) AS IRATE_FLOAT_TYPE ,
  COALESCE(FLOAT_IRATE, 0 ) AS FLOAT_IRATE ,
  COALESCE(FLOAT_POINTS, 0 ) AS FLOAT_POINTS ,
  COALESCE(IRATE_CODE, '' ) AS IRATE_CODE ,
  COALESCE(IRATE, 0 ) AS IRATE ,
  COALESCE(IRATE_ADJ_DATE, '' ) AS IRATE_ADJ_DATE ,
  COALESCE(IRATE_ADJUST_WAY, '' ) AS IRATE_ADJUST_WAY ,
  COALESCE(ADJ_CYCLE_TYPE, '' ) AS ADJ_CYCLE_TYPE ,
  COALESCE(ADJ_BEGIN_DATE, '' ) AS ADJ_BEGIN_DATE ,
  COALESCE(SPECIFIED_DATE, '' ) AS SPECIFIED_DATE ,
  COALESCE(ADJ_IWEGH, 0 ) AS ADJ_IWEGH ,
  COALESCE(ROOT_ETR_ACCT, '' ) AS ROOT_ETR_ACCT ,
  COALESCE(SETTL_ACCT, '' ) AS SETTL_ACCT ,
  COALESCE(VCHR_TYPE, '' ) AS VCHR_TYPE ,
  COALESCE(VCHR_NO, '' ) AS VCHR_NO ,
  COALESCE(PRTOVR_PAGERS, 0 ) AS PRTOVR_PAGERS ,
  COALESCE(NO_PRT_CONT, 0 ) AS NO_PRT_CONT ,
  COALESCE(AUTO_FLAG, '' ) AS AUTO_FLAG ,
  COALESCE(AUTO_TIMES, 0 ) AS AUTO_TIMES ,
  COALESCE(HAVE_TIMES, 0 ) AS HAVE_TIMES ,
  COALESCE(APPOINT_DRAW_FLAG, '' ) AS APPOINT_DRAW_FLAG ,
  COALESCE(APPOINT_ACCT, '' ) AS APPOINT_ACCT ,
  COALESCE(CUST_MGR_TYPE, '' ) AS CUST_MGR_TYPE ,
  COALESCE(CUST_MGR_NO, '' ) AS CUST_MGR_NO ,
  COALESCE(CUST_MGR_NAME, '' ) AS CUST_MGR_NAME ,
  COALESCE(DEPTERM_FLAG, '' ) AS DEPTERM_FLAG ,
  COALESCE(DAC, '' ) AS DAC 
 FROM  dw_tdata.CBS_001_AMMST_SECU_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  INTER_ACCT ,
  SUBACCT ,
  ACCOUNT ,
  CUR_CODE ,
  CUST_ID ,
  FUND_TYPE ,
  ACCT_NAME ,
  ACCT_ENAME ,
  OPEN_UNIT ,
  ACCT_KIND ,
  CASH_REMIT_FLAG ,
  SUBACCT_SYS ,
  SECU_PURPOSE ,
  DEP_TYPE ,
  ACCT_CHARA ,
  YDAY_BAL ,
  CUR_BAL ,
  BAL_FLAG ,
  DRAW_FLAG ,
  SEAL_CARD_NO ,
  OPEN_CONF_NO ,
  PAY_PSSWD_ORG ,
  CHECK_FLAG ,
  ACCT_STATE ,
  LOSS_FLAG ,
  FROZ_FLAG ,
  FROZ_AMT ,
  STOP_AMT ,
  CERT_AMT ,
  IMPAWN_AMT ,
  OPEN_DATE ,
  INT_START_DATE ,
  CHECK_DATE ,
  DEP_TERM ,
  CLOSE_DATE ,
  DUE_DATE ,
  BRK_DEP_NUM ,
  LAST_CI_DATE ,
  LST_CLR_DATE ,
  LST_OCC_DATE ,
  TIME_STAMP ,
  I_FLAG ,
  I_MTHD ,
  IRATE_KIND ,
  IRATE_LEVEL ,
  IRATE_FLOAT_TYPE ,
  FLOAT_IRATE ,
  FLOAT_POINTS ,
  IRATE_CODE ,
  IRATE ,
  IRATE_ADJ_DATE ,
  IRATE_ADJUST_WAY ,
  ADJ_CYCLE_TYPE ,
  ADJ_BEGIN_DATE ,
  SPECIFIED_DATE ,
  ADJ_IWEGH ,
  ROOT_ETR_ACCT ,
  SETTL_ACCT ,
  VCHR_TYPE ,
  VCHR_NO ,
  PRTOVR_PAGERS ,
  NO_PRT_CONT ,
  AUTO_FLAG ,
  AUTO_TIMES ,
  HAVE_TIMES ,
  APPOINT_DRAW_FLAG ,
  APPOINT_ACCT ,
  CUST_MGR_TYPE ,
  CUST_MGR_NO ,
  CUST_MGR_NAME ,
  DEPTERM_FLAG ,
  DAC 
 FROM dw_sdata.CBS_001_AMMST_SECU 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.INTER_ACCT = T.INTER_ACCT
WHERE
(T.INTER_ACCT IS NULL)
 OR N.SUBACCT<>T.SUBACCT
 OR N.ACCOUNT<>T.ACCOUNT
 OR N.CUR_CODE<>T.CUR_CODE
 OR N.CUST_ID<>T.CUST_ID
 OR N.FUND_TYPE<>T.FUND_TYPE
 OR N.ACCT_NAME<>T.ACCT_NAME
 OR N.ACCT_ENAME<>T.ACCT_ENAME
 OR N.OPEN_UNIT<>T.OPEN_UNIT
 OR N.ACCT_KIND<>T.ACCT_KIND
 OR N.CASH_REMIT_FLAG<>T.CASH_REMIT_FLAG
 OR N.SUBACCT_SYS<>T.SUBACCT_SYS
 OR N.SECU_PURPOSE<>T.SECU_PURPOSE
 OR N.DEP_TYPE<>T.DEP_TYPE
 OR N.ACCT_CHARA<>T.ACCT_CHARA
 OR N.YDAY_BAL<>T.YDAY_BAL
 OR N.CUR_BAL<>T.CUR_BAL
 OR N.BAL_FLAG<>T.BAL_FLAG
 OR N.DRAW_FLAG<>T.DRAW_FLAG
 OR N.SEAL_CARD_NO<>T.SEAL_CARD_NO
 OR N.OPEN_CONF_NO<>T.OPEN_CONF_NO
 OR N.PAY_PSSWD_ORG<>T.PAY_PSSWD_ORG
 OR N.CHECK_FLAG<>T.CHECK_FLAG
 OR N.ACCT_STATE<>T.ACCT_STATE
 OR N.LOSS_FLAG<>T.LOSS_FLAG
 OR N.FROZ_FLAG<>T.FROZ_FLAG
 OR N.FROZ_AMT<>T.FROZ_AMT
 OR N.STOP_AMT<>T.STOP_AMT
 OR N.CERT_AMT<>T.CERT_AMT
 OR N.IMPAWN_AMT<>T.IMPAWN_AMT
 OR N.OPEN_DATE<>T.OPEN_DATE
 OR N.INT_START_DATE<>T.INT_START_DATE
 OR N.CHECK_DATE<>T.CHECK_DATE
 OR N.DEP_TERM<>T.DEP_TERM
 OR N.CLOSE_DATE<>T.CLOSE_DATE
 OR N.DUE_DATE<>T.DUE_DATE
 OR N.BRK_DEP_NUM<>T.BRK_DEP_NUM
 OR N.LAST_CI_DATE<>T.LAST_CI_DATE
 OR N.LST_CLR_DATE<>T.LST_CLR_DATE
 OR N.LST_OCC_DATE<>T.LST_OCC_DATE
 OR N.TIME_STAMP<>T.TIME_STAMP
 OR N.I_FLAG<>T.I_FLAG
 OR N.I_MTHD<>T.I_MTHD
 OR N.IRATE_KIND<>T.IRATE_KIND
 OR N.IRATE_LEVEL<>T.IRATE_LEVEL
 OR N.IRATE_FLOAT_TYPE<>T.IRATE_FLOAT_TYPE
 OR N.FLOAT_IRATE<>T.FLOAT_IRATE
 OR N.FLOAT_POINTS<>T.FLOAT_POINTS
 OR N.IRATE_CODE<>T.IRATE_CODE
 OR N.IRATE<>T.IRATE
 OR N.IRATE_ADJ_DATE<>T.IRATE_ADJ_DATE
 OR N.IRATE_ADJUST_WAY<>T.IRATE_ADJUST_WAY
 OR N.ADJ_CYCLE_TYPE<>T.ADJ_CYCLE_TYPE
 OR N.ADJ_BEGIN_DATE<>T.ADJ_BEGIN_DATE
 OR N.SPECIFIED_DATE<>T.SPECIFIED_DATE
 OR N.ADJ_IWEGH<>T.ADJ_IWEGH
 OR N.ROOT_ETR_ACCT<>T.ROOT_ETR_ACCT
 OR N.SETTL_ACCT<>T.SETTL_ACCT
 OR N.VCHR_TYPE<>T.VCHR_TYPE
 OR N.VCHR_NO<>T.VCHR_NO
 OR N.PRTOVR_PAGERS<>T.PRTOVR_PAGERS
 OR N.NO_PRT_CONT<>T.NO_PRT_CONT
 OR N.AUTO_FLAG<>T.AUTO_FLAG
 OR N.AUTO_TIMES<>T.AUTO_TIMES
 OR N.HAVE_TIMES<>T.HAVE_TIMES
 OR N.APPOINT_DRAW_FLAG<>T.APPOINT_DRAW_FLAG
 OR N.APPOINT_ACCT<>T.APPOINT_ACCT
 OR N.CUST_MGR_TYPE<>T.CUST_MGR_TYPE
 OR N.CUST_MGR_NO<>T.CUST_MGR_NO
 OR N.CUST_MGR_NAME<>T.CUST_MGR_NAME
 OR N.DEPTERM_FLAG<>T.DEPTERM_FLAG
 OR N.DAC<>T.DAC
;

--Step3:
UPDATE dw_sdata.CBS_001_AMMST_SECU P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_71
WHERE P.End_Dt=DATE('2100-12-31')
AND P.INTER_ACCT=T_71.INTER_ACCT
;

--Step4:
INSERT  INTO dw_sdata.CBS_001_AMMST_SECU SELECT * FROM T_71;

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
