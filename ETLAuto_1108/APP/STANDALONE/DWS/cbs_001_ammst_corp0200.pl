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
DELETE FROM dw_sdata.CBS_001_AMMST_CORP WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CBS_001_AMMST_CORP SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_70 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CBS_001_AMMST_CORP WHERE 1=0;

--Step2:
INSERT  INTO T_70 (
  INTER_ACCT,
  SUBACCT,
  ACCOUNT,
  EBT_ACCT,
  CUR_CODE,
  CUST_ID,
  CUST_TYPE,
  CUST_CHARA,
  FUND_TYPE,
  ACCT_NAME,
  ACCT_ENAME,
  OPEN_UNIT,
  SPECIAL_UNIT_CODE,
  DTL_IND_TYPE,
  ACCT_KIND,
  CASH_REMIT_FLAG,
  OPEN_PAPER_NO,
  CLOSE_PAPER_NO,
  BAL_LIMIT_TYPE,
  BAL_LIMIT_CUR_CODE,
  BAL_LIMIT,
  BAL_LIMIT_DISCOUNT,
  SUM_CREDIT_BAL,
  DEP_TYPE,
  ACCT_CHARA,
  TMP_ACC_FLAG,
  ACC_INVALID_DATE,
  YDAY_BAL,
  CUR_BAL,
  BAL_FLAG,
  DRAW_FLAG,
  SEAL_CARD_NO,
  OPEN_CONF_NO,
  PAY_PSSWD_ORG,
  TC_WAY,
  TD_WAY,
  CHECK_FLAG,
  REV_PAY_FLAG,
  GROUPACCT_FLAG,
  ACCT_STATE,
  REC_FLAG,
  LOSS_FLAG,
  FROZ_FLAG,
  FROZ_AMT,
  STOP_AMT,
  CERT_AMT,
  IMPAWN_AMT,
  PERMIT_FLAG,
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
  OVRD_FLAG,
  OVRD_INTCODE,
  OVDR_IRATE,
  OVDR_IWEIT,
  FIN_CODE,
  ADJ_IWEGH,
  CASH_FLAG,
  CASH_MAXAMT,
  ROOT_ETR_ACCT,
  SETTL_ACCT,
  VCHR_TYPE,
  VCHR_NO,
  TRSF_LIMIT,
  CASH_LIMIT,
  PRTOVR_PAGERS,
  NO_PRT_CONT,
  EFFECT_DATE,
  AUTO_FLAG,
  AUTO_TIMES,
  HAVE_TIMES,
  APPOINT_DRAW_FLAG,
  APPOINT_ACCT,
  UNIT_ATTR,
  CUST_MGR_TYPE,
  CUST_MGR_NO,
  CUST_MGR_NAME,
  PERMIT_DATE,
  DEPTERM_FLAG,
  DAC,
  start_dt,
  end_dt)
SELECT
  N.INTER_ACCT,
  N.SUBACCT,
  N.ACCOUNT,
  N.EBT_ACCT,
  N.CUR_CODE,
  N.CUST_ID,
  N.CUST_TYPE,
  N.CUST_CHARA,
  N.FUND_TYPE,
  N.ACCT_NAME,
  N.ACCT_ENAME,
  N.OPEN_UNIT,
  N.SPECIAL_UNIT_CODE,
  N.DTL_IND_TYPE,
  N.ACCT_KIND,
  N.CASH_REMIT_FLAG,
  N.OPEN_PAPER_NO,
  N.CLOSE_PAPER_NO,
  N.BAL_LIMIT_TYPE,
  N.BAL_LIMIT_CUR_CODE,
  N.BAL_LIMIT,
  N.BAL_LIMIT_DISCOUNT,
  N.SUM_CREDIT_BAL,
  N.DEP_TYPE,
  N.ACCT_CHARA,
  N.TMP_ACC_FLAG,
  N.ACC_INVALID_DATE,
  N.YDAY_BAL,
  N.CUR_BAL,
  N.BAL_FLAG,
  N.DRAW_FLAG,
  N.SEAL_CARD_NO,
  N.OPEN_CONF_NO,
  N.PAY_PSSWD_ORG,
  N.TC_WAY,
  N.TD_WAY,
  N.CHECK_FLAG,
  N.REV_PAY_FLAG,
  N.GROUPACCT_FLAG,
  N.ACCT_STATE,
  N.REC_FLAG,
  N.LOSS_FLAG,
  N.FROZ_FLAG,
  N.FROZ_AMT,
  N.STOP_AMT,
  N.CERT_AMT,
  N.IMPAWN_AMT,
  N.PERMIT_FLAG,
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
  N.OVRD_FLAG,
  N.OVRD_INTCODE,
  N.OVDR_IRATE,
  N.OVDR_IWEIT,
  N.FIN_CODE,
  N.ADJ_IWEGH,
  N.CASH_FLAG,
  N.CASH_MAXAMT,
  N.ROOT_ETR_ACCT,
  N.SETTL_ACCT,
  N.VCHR_TYPE,
  N.VCHR_NO,
  N.TRSF_LIMIT,
  N.CASH_LIMIT,
  N.PRTOVR_PAGERS,
  N.NO_PRT_CONT,
  N.EFFECT_DATE,
  N.AUTO_FLAG,
  N.AUTO_TIMES,
  N.HAVE_TIMES,
  N.APPOINT_DRAW_FLAG,
  N.APPOINT_ACCT,
  N.UNIT_ATTR,
  N.CUST_MGR_TYPE,
  N.CUST_MGR_NO,
  N.CUST_MGR_NAME,
  N.PERMIT_DATE,
  N.DEPTERM_FLAG,
  N.DAC,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(INTER_ACCT, '' ) AS INTER_ACCT ,
  COALESCE(SUBACCT, 0 ) AS SUBACCT ,
  COALESCE(ACCOUNT, '' ) AS ACCOUNT ,
  COALESCE(EBT_ACCT, '' ) AS EBT_ACCT ,
  COALESCE(CUR_CODE, '' ) AS CUR_CODE ,
  COALESCE(CUST_ID, '' ) AS CUST_ID ,
  COALESCE(CUST_TYPE, '' ) AS CUST_TYPE ,
  COALESCE(CUST_CHARA, '' ) AS CUST_CHARA ,
  COALESCE(FUND_TYPE, '' ) AS FUND_TYPE ,
  COALESCE(ACCT_NAME, '' ) AS ACCT_NAME ,
  COALESCE(ACCT_ENAME, '' ) AS ACCT_ENAME ,
  COALESCE(OPEN_UNIT, '' ) AS OPEN_UNIT ,
  COALESCE(SPECIAL_UNIT_CODE, '' ) AS SPECIAL_UNIT_CODE ,
  COALESCE(DTL_IND_TYPE, '' ) AS DTL_IND_TYPE ,
  COALESCE(ACCT_KIND, '' ) AS ACCT_KIND ,
  COALESCE(CASH_REMIT_FLAG, '' ) AS CASH_REMIT_FLAG ,
  COALESCE(OPEN_PAPER_NO, '' ) AS OPEN_PAPER_NO ,
  COALESCE(CLOSE_PAPER_NO, '' ) AS CLOSE_PAPER_NO ,
  COALESCE(BAL_LIMIT_TYPE, '' ) AS BAL_LIMIT_TYPE ,
  COALESCE(BAL_LIMIT_CUR_CODE, '' ) AS BAL_LIMIT_CUR_CODE ,
  COALESCE(BAL_LIMIT, 0 ) AS BAL_LIMIT ,
  COALESCE(BAL_LIMIT_DISCOUNT, 0 ) AS BAL_LIMIT_DISCOUNT ,
  COALESCE(SUM_CREDIT_BAL, 0 ) AS SUM_CREDIT_BAL ,
  COALESCE(DEP_TYPE, '' ) AS DEP_TYPE ,
  COALESCE(ACCT_CHARA, '' ) AS ACCT_CHARA ,
  COALESCE(TMP_ACC_FLAG, '' ) AS TMP_ACC_FLAG ,
  COALESCE(ACC_INVALID_DATE, '' ) AS ACC_INVALID_DATE ,
  COALESCE(YDAY_BAL, 0 ) AS YDAY_BAL ,
  COALESCE(CUR_BAL, 0 ) AS CUR_BAL ,
  COALESCE(BAL_FLAG, '' ) AS BAL_FLAG ,
  COALESCE(DRAW_FLAG, '' ) AS DRAW_FLAG ,
  COALESCE(SEAL_CARD_NO, '' ) AS SEAL_CARD_NO ,
  COALESCE(OPEN_CONF_NO, '' ) AS OPEN_CONF_NO ,
  COALESCE(PAY_PSSWD_ORG, '' ) AS PAY_PSSWD_ORG ,
  COALESCE(TC_WAY, '' ) AS TC_WAY ,
  COALESCE(TD_WAY, '' ) AS TD_WAY ,
  COALESCE(CHECK_FLAG, '' ) AS CHECK_FLAG ,
  COALESCE(REV_PAY_FLAG, '' ) AS REV_PAY_FLAG ,
  COALESCE(GROUPACCT_FLAG, '' ) AS GROUPACCT_FLAG ,
  COALESCE(ACCT_STATE, '' ) AS ACCT_STATE ,
  COALESCE(REC_FLAG, '' ) AS REC_FLAG ,
  COALESCE(LOSS_FLAG, '' ) AS LOSS_FLAG ,
  COALESCE(FROZ_FLAG, '' ) AS FROZ_FLAG ,
  COALESCE(FROZ_AMT, 0 ) AS FROZ_AMT ,
  COALESCE(STOP_AMT, 0 ) AS STOP_AMT ,
  COALESCE(CERT_AMT, 0 ) AS CERT_AMT ,
  COALESCE(IMPAWN_AMT, 0 ) AS IMPAWN_AMT ,
  COALESCE(PERMIT_FLAG, '' ) AS PERMIT_FLAG ,
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
  COALESCE(OVRD_FLAG, '' ) AS OVRD_FLAG ,
  COALESCE(OVRD_INTCODE, '' ) AS OVRD_INTCODE ,
  COALESCE(OVDR_IRATE, 0 ) AS OVDR_IRATE ,
  COALESCE(OVDR_IWEIT, 0 ) AS OVDR_IWEIT ,
  COALESCE(FIN_CODE, '' ) AS FIN_CODE ,
  COALESCE(ADJ_IWEGH, 0 ) AS ADJ_IWEGH ,
  COALESCE(CASH_FLAG, '' ) AS CASH_FLAG ,
  COALESCE(CASH_MAXAMT, 0 ) AS CASH_MAXAMT ,
  COALESCE(ROOT_ETR_ACCT, '' ) AS ROOT_ETR_ACCT ,
  COALESCE(SETTL_ACCT, '' ) AS SETTL_ACCT ,
  COALESCE(VCHR_TYPE, '' ) AS VCHR_TYPE ,
  COALESCE(VCHR_NO, '' ) AS VCHR_NO ,
  COALESCE(TRSF_LIMIT, 0 ) AS TRSF_LIMIT ,
  COALESCE(CASH_LIMIT, 0 ) AS CASH_LIMIT ,
  COALESCE(PRTOVR_PAGERS, 0 ) AS PRTOVR_PAGERS ,
  COALESCE(NO_PRT_CONT, 0 ) AS NO_PRT_CONT ,
  COALESCE(EFFECT_DATE, '' ) AS EFFECT_DATE ,
  COALESCE(AUTO_FLAG, '' ) AS AUTO_FLAG ,
  COALESCE(AUTO_TIMES, 0 ) AS AUTO_TIMES ,
  COALESCE(HAVE_TIMES, 0 ) AS HAVE_TIMES ,
  COALESCE(APPOINT_DRAW_FLAG, '' ) AS APPOINT_DRAW_FLAG ,
  COALESCE(APPOINT_ACCT, '' ) AS APPOINT_ACCT ,
  COALESCE(UNIT_ATTR, '' ) AS UNIT_ATTR ,
  COALESCE(CUST_MGR_TYPE, '' ) AS CUST_MGR_TYPE ,
  COALESCE(CUST_MGR_NO, '' ) AS CUST_MGR_NO ,
  COALESCE(CUST_MGR_NAME, '' ) AS CUST_MGR_NAME ,
  COALESCE(PERMIT_DATE, '' ) AS PERMIT_DATE ,
  COALESCE(DEPTERM_FLAG, '' ) AS DEPTERM_FLAG ,
  COALESCE(DAC, '' ) AS DAC 
 FROM  dw_tdata.CBS_001_AMMST_CORP_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  INTER_ACCT ,
  SUBACCT ,
  ACCOUNT ,
  EBT_ACCT ,
  CUR_CODE ,
  CUST_ID ,
  CUST_TYPE ,
  CUST_CHARA ,
  FUND_TYPE ,
  ACCT_NAME ,
  ACCT_ENAME ,
  OPEN_UNIT ,
  SPECIAL_UNIT_CODE ,
  DTL_IND_TYPE ,
  ACCT_KIND ,
  CASH_REMIT_FLAG ,
  OPEN_PAPER_NO ,
  CLOSE_PAPER_NO ,
  BAL_LIMIT_TYPE ,
  BAL_LIMIT_CUR_CODE ,
  BAL_LIMIT ,
  BAL_LIMIT_DISCOUNT ,
  SUM_CREDIT_BAL ,
  DEP_TYPE ,
  ACCT_CHARA ,
  TMP_ACC_FLAG ,
  ACC_INVALID_DATE ,
  YDAY_BAL ,
  CUR_BAL ,
  BAL_FLAG ,
  DRAW_FLAG ,
  SEAL_CARD_NO ,
  OPEN_CONF_NO ,
  PAY_PSSWD_ORG ,
  TC_WAY ,
  TD_WAY ,
  CHECK_FLAG ,
  REV_PAY_FLAG ,
  GROUPACCT_FLAG ,
  ACCT_STATE ,
  REC_FLAG ,
  LOSS_FLAG ,
  FROZ_FLAG ,
  FROZ_AMT ,
  STOP_AMT ,
  CERT_AMT ,
  IMPAWN_AMT ,
  PERMIT_FLAG ,
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
  OVRD_FLAG ,
  OVRD_INTCODE ,
  OVDR_IRATE ,
  OVDR_IWEIT ,
  FIN_CODE ,
  ADJ_IWEGH ,
  CASH_FLAG ,
  CASH_MAXAMT ,
  ROOT_ETR_ACCT ,
  SETTL_ACCT ,
  VCHR_TYPE ,
  VCHR_NO ,
  TRSF_LIMIT ,
  CASH_LIMIT ,
  PRTOVR_PAGERS ,
  NO_PRT_CONT ,
  EFFECT_DATE ,
  AUTO_FLAG ,
  AUTO_TIMES ,
  HAVE_TIMES ,
  APPOINT_DRAW_FLAG ,
  APPOINT_ACCT ,
  UNIT_ATTR ,
  CUST_MGR_TYPE ,
  CUST_MGR_NO ,
  CUST_MGR_NAME ,
  PERMIT_DATE ,
  DEPTERM_FLAG ,
  DAC 
 FROM dw_sdata.CBS_001_AMMST_CORP 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.INTER_ACCT = T.INTER_ACCT
WHERE
(T.INTER_ACCT IS NULL)
 OR N.SUBACCT<>T.SUBACCT
 OR N.ACCOUNT<>T.ACCOUNT
 OR N.EBT_ACCT<>T.EBT_ACCT
 OR N.CUR_CODE<>T.CUR_CODE
 OR N.CUST_ID<>T.CUST_ID
 OR N.CUST_TYPE<>T.CUST_TYPE
 OR N.CUST_CHARA<>T.CUST_CHARA
 OR N.FUND_TYPE<>T.FUND_TYPE
 OR N.ACCT_NAME<>T.ACCT_NAME
 OR N.ACCT_ENAME<>T.ACCT_ENAME
 OR N.OPEN_UNIT<>T.OPEN_UNIT
 OR N.SPECIAL_UNIT_CODE<>T.SPECIAL_UNIT_CODE
 OR N.DTL_IND_TYPE<>T.DTL_IND_TYPE
 OR N.ACCT_KIND<>T.ACCT_KIND
 OR N.CASH_REMIT_FLAG<>T.CASH_REMIT_FLAG
 OR N.OPEN_PAPER_NO<>T.OPEN_PAPER_NO
 OR N.CLOSE_PAPER_NO<>T.CLOSE_PAPER_NO
 OR N.BAL_LIMIT_TYPE<>T.BAL_LIMIT_TYPE
 OR N.BAL_LIMIT_CUR_CODE<>T.BAL_LIMIT_CUR_CODE
 OR N.BAL_LIMIT<>T.BAL_LIMIT
 OR N.BAL_LIMIT_DISCOUNT<>T.BAL_LIMIT_DISCOUNT
 OR N.SUM_CREDIT_BAL<>T.SUM_CREDIT_BAL
 OR N.DEP_TYPE<>T.DEP_TYPE
 OR N.ACCT_CHARA<>T.ACCT_CHARA
 OR N.TMP_ACC_FLAG<>T.TMP_ACC_FLAG
 OR N.ACC_INVALID_DATE<>T.ACC_INVALID_DATE
 OR N.YDAY_BAL<>T.YDAY_BAL
 OR N.CUR_BAL<>T.CUR_BAL
 OR N.BAL_FLAG<>T.BAL_FLAG
 OR N.DRAW_FLAG<>T.DRAW_FLAG
 OR N.SEAL_CARD_NO<>T.SEAL_CARD_NO
 OR N.OPEN_CONF_NO<>T.OPEN_CONF_NO
 OR N.PAY_PSSWD_ORG<>T.PAY_PSSWD_ORG
 OR N.TC_WAY<>T.TC_WAY
 OR N.TD_WAY<>T.TD_WAY
 OR N.CHECK_FLAG<>T.CHECK_FLAG
 OR N.REV_PAY_FLAG<>T.REV_PAY_FLAG
 OR N.GROUPACCT_FLAG<>T.GROUPACCT_FLAG
 OR N.ACCT_STATE<>T.ACCT_STATE
 OR N.REC_FLAG<>T.REC_FLAG
 OR N.LOSS_FLAG<>T.LOSS_FLAG
 OR N.FROZ_FLAG<>T.FROZ_FLAG
 OR N.FROZ_AMT<>T.FROZ_AMT
 OR N.STOP_AMT<>T.STOP_AMT
 OR N.CERT_AMT<>T.CERT_AMT
 OR N.IMPAWN_AMT<>T.IMPAWN_AMT
 OR N.PERMIT_FLAG<>T.PERMIT_FLAG
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
 OR N.OVRD_FLAG<>T.OVRD_FLAG
 OR N.OVRD_INTCODE<>T.OVRD_INTCODE
 OR N.OVDR_IRATE<>T.OVDR_IRATE
 OR N.OVDR_IWEIT<>T.OVDR_IWEIT
 OR N.FIN_CODE<>T.FIN_CODE
 OR N.ADJ_IWEGH<>T.ADJ_IWEGH
 OR N.CASH_FLAG<>T.CASH_FLAG
 OR N.CASH_MAXAMT<>T.CASH_MAXAMT
 OR N.ROOT_ETR_ACCT<>T.ROOT_ETR_ACCT
 OR N.SETTL_ACCT<>T.SETTL_ACCT
 OR N.VCHR_TYPE<>T.VCHR_TYPE
 OR N.VCHR_NO<>T.VCHR_NO
 OR N.TRSF_LIMIT<>T.TRSF_LIMIT
 OR N.CASH_LIMIT<>T.CASH_LIMIT
 OR N.PRTOVR_PAGERS<>T.PRTOVR_PAGERS
 OR N.NO_PRT_CONT<>T.NO_PRT_CONT
 OR N.EFFECT_DATE<>T.EFFECT_DATE
 OR N.AUTO_FLAG<>T.AUTO_FLAG
 OR N.AUTO_TIMES<>T.AUTO_TIMES
 OR N.HAVE_TIMES<>T.HAVE_TIMES
 OR N.APPOINT_DRAW_FLAG<>T.APPOINT_DRAW_FLAG
 OR N.APPOINT_ACCT<>T.APPOINT_ACCT
 OR N.UNIT_ATTR<>T.UNIT_ATTR
 OR N.CUST_MGR_TYPE<>T.CUST_MGR_TYPE
 OR N.CUST_MGR_NO<>T.CUST_MGR_NO
 OR N.CUST_MGR_NAME<>T.CUST_MGR_NAME
 OR N.PERMIT_DATE<>T.PERMIT_DATE
 OR N.DEPTERM_FLAG<>T.DEPTERM_FLAG
 OR N.DAC<>T.DAC
;

--Step3:
UPDATE dw_sdata.CBS_001_AMMST_CORP P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_70
WHERE P.End_Dt=DATE('2100-12-31')
AND P.INTER_ACCT=T_70.INTER_ACCT
;

--Step4:
INSERT  INTO dw_sdata.CBS_001_AMMST_CORP SELECT * FROM T_70;

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