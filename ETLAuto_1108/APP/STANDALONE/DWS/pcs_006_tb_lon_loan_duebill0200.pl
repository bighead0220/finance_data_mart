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
DELETE FROM dw_sdata.PCS_006_TB_LON_LOAN_DUEBILL WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.PCS_006_TB_LON_LOAN_DUEBILL SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_338 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.PCS_006_TB_LON_LOAN_DUEBILL WHERE 1=0;

--Step2:
INSERT  INTO T_338 (
  LOAN_ID,
  LOAN_CONTRACT_ID,
  FLOW_NO,
  DUEBILL_ID,
  DUEBILL_NO,
  DUEBILL_AMOUNT,
  DUEBILL_BALANCE,
  DUEBILL_INTEREST_BALANCE,
  DUEBILL_RATE,
  CURRENCY,
  NOTICE_NO,
  DUEBILL_BEGIN_DATE,
  DUEBILL_END_DATE,
  ACCOUNT_NO,
  EXTENSION_FLAG,
  EXTENSION_DATE,
  DUEBILL_STATUS,
  LOAN_LEVEL_FIVE_CLASS,
  DUEBILL_BALANCE_NORMAL,
  DUEBILL_BALANCE_OVERDUE,
  DUEBILL_BALANCE_DULL,
  DUEBILL_BALANCE_BAD,
  CANCEL_CAPITAL_BALANCE,
  CANCEL_IN_SHEET_BALANCE,
  CANCEL_OFF_SHEET_BALANCE,
  IN_SHEET_INTEREST_BALANCE,
  OFF_SHEET_INTEREST_BALANCE,
  INTEREST_INCOME,
  COUNT_OVERDUE_TERM,
  CURRENT_OVERDUE_TERM,
  MAX_OVERDUE_TERM,
  ALLOW_MAX_OVERDUE_DAY,
  CLOSE_FLAG,
  OLD_DUEBILL_ID,
  OLD_DUEBILL_NO,
  PAYEE_NO,
  STOP_PAY_NO,
  REPAY_NO,
  LOAN_STOP_PAY_NO,
  TRANS_FLOW_NO,
  CHECK_DATE,
  LOCAL_DATE,
  LOCAL_TIME,
  IS_STOP_INTEREST_FLAG,
  IS_DISCOUNT,
  BEGIN_REPAY_CAPITAL_MONTH,
  IS_LAW_STOP_INTEREST_FLAG,
  IS_CARD_LOAN,
  IS_CARD_REPAY,
  IS_CARD_TRANSFER,
  ACCOUNT_NAME,
  REPAY_NAME,
  PAYEE_NAME,
  NORMAL_IN_SHEET_INTEREST,
  NORMAL_OFF_SHEET_INTEREST,
  OVERDUE_IN_SHEET_INTEREST,
  OVERDUE_OFF_SHEET_INTEREST,
  DEFAULT_IN_SHEET_INTEREST,
  DEFAULT_OFF_SHEET_INTEREST,
  CANCEL_CAPITAL,
  CANCEL_INTEREST,
  CAPITAL_AMOUNT,
  END_SUM_AMOUNT,
  DEBT_SUM_EVALUATE,
  CANCEL_ACCRUED_INTEREST,
  PROJECT_ID,
  FRANK,
  ENTRUST_PAYMENT_STATUS,
  PAYMENT_WAY,
  OVERDUE_RATE,
  PROVINCE_NUM,
  NORMAL_PAYMENT_AMOUNT,
  ENTRUST_PAYMENT_AMOUNT,
  SPECIAL_PAYMENT_AMOUNT,
  DELFLAG,
  CREATE_TIME,
  UPDATE_TIME,
  TRUNC_NO,
  EFF_CURR_PERI,
  DISCOUNT_STATUS,
  OVERDUE_DAYS,
  CANCEL_DATE,
  SPAREINTERESTFALG,
  OVERDUE_AMOUNT,
  OVERDUE_RATE_AMOUNT,
  OVERDUE_TOTAL,
  CLOSE_DATE,
  LOCK_FLAG,
  IS_STOP_REPAYMENT,
  BIZ_CHANNEL_KIND,
  BIZ_CHANNEL_SUB_KIND,
  GSYS_CENTER_NUM,
  GSYS_BANK_ORG,
  GSYS_DUEBILL_NO,
  RECORD_DATE,
  start_dt,
  end_dt)
SELECT
  N.LOAN_ID,
  N.LOAN_CONTRACT_ID,
  N.FLOW_NO,
  N.DUEBILL_ID,
  N.DUEBILL_NO,
  N.DUEBILL_AMOUNT,
  N.DUEBILL_BALANCE,
  N.DUEBILL_INTEREST_BALANCE,
  N.DUEBILL_RATE,
  N.CURRENCY,
  N.NOTICE_NO,
  N.DUEBILL_BEGIN_DATE,
  N.DUEBILL_END_DATE,
  N.ACCOUNT_NO,
  N.EXTENSION_FLAG,
  N.EXTENSION_DATE,
  N.DUEBILL_STATUS,
  N.LOAN_LEVEL_FIVE_CLASS,
  N.DUEBILL_BALANCE_NORMAL,
  N.DUEBILL_BALANCE_OVERDUE,
  N.DUEBILL_BALANCE_DULL,
  N.DUEBILL_BALANCE_BAD,
  N.CANCEL_CAPITAL_BALANCE,
  N.CANCEL_IN_SHEET_BALANCE,
  N.CANCEL_OFF_SHEET_BALANCE,
  N.IN_SHEET_INTEREST_BALANCE,
  N.OFF_SHEET_INTEREST_BALANCE,
  N.INTEREST_INCOME,
  N.COUNT_OVERDUE_TERM,
  N.CURRENT_OVERDUE_TERM,
  N.MAX_OVERDUE_TERM,
  N.ALLOW_MAX_OVERDUE_DAY,
  N.CLOSE_FLAG,
  N.OLD_DUEBILL_ID,
  N.OLD_DUEBILL_NO,
  N.PAYEE_NO,
  N.STOP_PAY_NO,
  N.REPAY_NO,
  N.LOAN_STOP_PAY_NO,
  N.TRANS_FLOW_NO,
  N.CHECK_DATE,
  N.LOCAL_DATE,
  N.LOCAL_TIME,
  N.IS_STOP_INTEREST_FLAG,
  N.IS_DISCOUNT,
  N.BEGIN_REPAY_CAPITAL_MONTH,
  N.IS_LAW_STOP_INTEREST_FLAG,
  N.IS_CARD_LOAN,
  N.IS_CARD_REPAY,
  N.IS_CARD_TRANSFER,
  N.ACCOUNT_NAME,
  N.REPAY_NAME,
  N.PAYEE_NAME,
  N.NORMAL_IN_SHEET_INTEREST,
  N.NORMAL_OFF_SHEET_INTEREST,
  N.OVERDUE_IN_SHEET_INTEREST,
  N.OVERDUE_OFF_SHEET_INTEREST,
  N.DEFAULT_IN_SHEET_INTEREST,
  N.DEFAULT_OFF_SHEET_INTEREST,
  N.CANCEL_CAPITAL,
  N.CANCEL_INTEREST,
  N.CAPITAL_AMOUNT,
  N.END_SUM_AMOUNT,
  N.DEBT_SUM_EVALUATE,
  N.CANCEL_ACCRUED_INTEREST,
  N.PROJECT_ID,
  N.FRANK,
  N.ENTRUST_PAYMENT_STATUS,
  N.PAYMENT_WAY,
  N.OVERDUE_RATE,
  N.PROVINCE_NUM,
  N.NORMAL_PAYMENT_AMOUNT,
  N.ENTRUST_PAYMENT_AMOUNT,
  N.SPECIAL_PAYMENT_AMOUNT,
  N.DELFLAG,
  N.CREATE_TIME,
  N.UPDATE_TIME,
  N.TRUNC_NO,
  N.EFF_CURR_PERI,
  N.DISCOUNT_STATUS,
  N.OVERDUE_DAYS,
  N.CANCEL_DATE,
  N.SPAREINTERESTFALG,
  N.OVERDUE_AMOUNT,
  N.OVERDUE_RATE_AMOUNT,
  N.OVERDUE_TOTAL,
  N.CLOSE_DATE,
  N.LOCK_FLAG,
  N.IS_STOP_REPAYMENT,
  N.BIZ_CHANNEL_KIND,
  N.BIZ_CHANNEL_SUB_KIND,
  N.GSYS_CENTER_NUM,
  N.GSYS_BANK_ORG,
  N.GSYS_DUEBILL_NO,
  N.RECORD_DATE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(LOAN_ID, '' ) AS LOAN_ID ,
  COALESCE(LOAN_CONTRACT_ID, '' ) AS LOAN_CONTRACT_ID ,
  COALESCE(FLOW_NO, '' ) AS FLOW_NO ,
  COALESCE(DUEBILL_ID, '' ) AS DUEBILL_ID ,
  COALESCE(DUEBILL_NO, '' ) AS DUEBILL_NO ,
  COALESCE(DUEBILL_AMOUNT, 0 ) AS DUEBILL_AMOUNT ,
  COALESCE(DUEBILL_BALANCE, 0 ) AS DUEBILL_BALANCE ,
  COALESCE(DUEBILL_INTEREST_BALANCE, 0 ) AS DUEBILL_INTEREST_BALANCE ,
  COALESCE(DUEBILL_RATE, 0 ) AS DUEBILL_RATE ,
  COALESCE(CURRENCY, '' ) AS CURRENCY ,
  COALESCE(NOTICE_NO, '' ) AS NOTICE_NO ,
  COALESCE(DUEBILL_BEGIN_DATE,DATE('4999-12-31') ) AS DUEBILL_BEGIN_DATE ,
  COALESCE(DUEBILL_END_DATE,DATE('4999-12-31') ) AS DUEBILL_END_DATE ,
  COALESCE(ACCOUNT_NO, '' ) AS ACCOUNT_NO ,
  COALESCE(EXTENSION_FLAG, '' ) AS EXTENSION_FLAG ,
  COALESCE(EXTENSION_DATE,DATE('4999-12-31') ) AS EXTENSION_DATE ,
  COALESCE(DUEBILL_STATUS, '' ) AS DUEBILL_STATUS ,
  COALESCE(LOAN_LEVEL_FIVE_CLASS, '' ) AS LOAN_LEVEL_FIVE_CLASS ,
  COALESCE(DUEBILL_BALANCE_NORMAL, 0 ) AS DUEBILL_BALANCE_NORMAL ,
  COALESCE(DUEBILL_BALANCE_OVERDUE, 0 ) AS DUEBILL_BALANCE_OVERDUE ,
  COALESCE(DUEBILL_BALANCE_DULL, 0 ) AS DUEBILL_BALANCE_DULL ,
  COALESCE(DUEBILL_BALANCE_BAD, 0 ) AS DUEBILL_BALANCE_BAD ,
  COALESCE(CANCEL_CAPITAL_BALANCE, 0 ) AS CANCEL_CAPITAL_BALANCE ,
  COALESCE(CANCEL_IN_SHEET_BALANCE, 0 ) AS CANCEL_IN_SHEET_BALANCE ,
  COALESCE(CANCEL_OFF_SHEET_BALANCE, 0 ) AS CANCEL_OFF_SHEET_BALANCE ,
  COALESCE(IN_SHEET_INTEREST_BALANCE, 0 ) AS IN_SHEET_INTEREST_BALANCE ,
  COALESCE(OFF_SHEET_INTEREST_BALANCE, 0 ) AS OFF_SHEET_INTEREST_BALANCE ,
  COALESCE(INTEREST_INCOME, 0 ) AS INTEREST_INCOME ,
  COALESCE(COUNT_OVERDUE_TERM, '' ) AS COUNT_OVERDUE_TERM ,
  COALESCE(CURRENT_OVERDUE_TERM, '' ) AS CURRENT_OVERDUE_TERM ,
  COALESCE(MAX_OVERDUE_TERM, '' ) AS MAX_OVERDUE_TERM ,
  COALESCE(ALLOW_MAX_OVERDUE_DAY, 0 ) AS ALLOW_MAX_OVERDUE_DAY ,
  COALESCE(CLOSE_FLAG, '' ) AS CLOSE_FLAG ,
  COALESCE(OLD_DUEBILL_ID, '' ) AS OLD_DUEBILL_ID ,
  COALESCE(OLD_DUEBILL_NO, '' ) AS OLD_DUEBILL_NO ,
  COALESCE(PAYEE_NO, '' ) AS PAYEE_NO ,
  COALESCE(STOP_PAY_NO, '' ) AS STOP_PAY_NO ,
  COALESCE(REPAY_NO, '' ) AS REPAY_NO ,
  COALESCE(LOAN_STOP_PAY_NO, '' ) AS LOAN_STOP_PAY_NO ,
  COALESCE(TRANS_FLOW_NO, '' ) AS TRANS_FLOW_NO ,
  COALESCE(CHECK_DATE,DATE('4999-12-31') ) AS CHECK_DATE ,
  COALESCE(LOCAL_DATE, '' ) AS LOCAL_DATE ,
  COALESCE(LOCAL_TIME, '' ) AS LOCAL_TIME ,
  COALESCE(IS_STOP_INTEREST_FLAG, '' ) AS IS_STOP_INTEREST_FLAG ,
  COALESCE(IS_DISCOUNT, '' ) AS IS_DISCOUNT ,
  COALESCE(BEGIN_REPAY_CAPITAL_MONTH, '' ) AS BEGIN_REPAY_CAPITAL_MONTH ,
  COALESCE(IS_LAW_STOP_INTEREST_FLAG, '' ) AS IS_LAW_STOP_INTEREST_FLAG ,
  COALESCE(IS_CARD_LOAN, '' ) AS IS_CARD_LOAN ,
  COALESCE(IS_CARD_REPAY, '' ) AS IS_CARD_REPAY ,
  COALESCE(IS_CARD_TRANSFER, '' ) AS IS_CARD_TRANSFER ,
  COALESCE(ACCOUNT_NAME, '' ) AS ACCOUNT_NAME ,
  COALESCE(REPAY_NAME, '' ) AS REPAY_NAME ,
  COALESCE(PAYEE_NAME, '' ) AS PAYEE_NAME ,
  COALESCE(NORMAL_IN_SHEET_INTEREST, 0 ) AS NORMAL_IN_SHEET_INTEREST ,
  COALESCE(NORMAL_OFF_SHEET_INTEREST, 0 ) AS NORMAL_OFF_SHEET_INTEREST ,
  COALESCE(OVERDUE_IN_SHEET_INTEREST, 0 ) AS OVERDUE_IN_SHEET_INTEREST ,
  COALESCE(OVERDUE_OFF_SHEET_INTEREST, 0 ) AS OVERDUE_OFF_SHEET_INTEREST ,
  COALESCE(DEFAULT_IN_SHEET_INTEREST, 0 ) AS DEFAULT_IN_SHEET_INTEREST ,
  COALESCE(DEFAULT_OFF_SHEET_INTEREST, 0 ) AS DEFAULT_OFF_SHEET_INTEREST ,
  COALESCE(CANCEL_CAPITAL, 0 ) AS CANCEL_CAPITAL ,
  COALESCE(CANCEL_INTEREST, 0 ) AS CANCEL_INTEREST ,
  COALESCE(CAPITAL_AMOUNT, 0 ) AS CAPITAL_AMOUNT ,
  COALESCE(END_SUM_AMOUNT, 0 ) AS END_SUM_AMOUNT ,
  COALESCE(DEBT_SUM_EVALUATE, 0 ) AS DEBT_SUM_EVALUATE ,
  COALESCE(CANCEL_ACCRUED_INTEREST, 0 ) AS CANCEL_ACCRUED_INTEREST ,
  COALESCE(PROJECT_ID, '' ) AS PROJECT_ID ,
  COALESCE(FRANK, 0 ) AS FRANK ,
  COALESCE(ENTRUST_PAYMENT_STATUS, '' ) AS ENTRUST_PAYMENT_STATUS ,
  COALESCE(PAYMENT_WAY, '' ) AS PAYMENT_WAY ,
  COALESCE(OVERDUE_RATE, 0 ) AS OVERDUE_RATE ,
  COALESCE(PROVINCE_NUM, '' ) AS PROVINCE_NUM ,
  COALESCE(NORMAL_PAYMENT_AMOUNT, 0 ) AS NORMAL_PAYMENT_AMOUNT ,
  COALESCE(ENTRUST_PAYMENT_AMOUNT, 0 ) AS ENTRUST_PAYMENT_AMOUNT ,
  COALESCE(SPECIAL_PAYMENT_AMOUNT, 0 ) AS SPECIAL_PAYMENT_AMOUNT ,
  COALESCE(DELFLAG, '' ) AS DELFLAG ,
  COALESCE(CREATE_TIME,'4999-12-31 00:00:00' ) AS CREATE_TIME ,
  COALESCE(UPDATE_TIME,'4999-12-31 00:00:00' ) AS UPDATE_TIME ,
  COALESCE(TRUNC_NO, 0 ) AS TRUNC_NO ,
  COALESCE(EFF_CURR_PERI, 0 ) AS EFF_CURR_PERI ,
  COALESCE(DISCOUNT_STATUS, '' ) AS DISCOUNT_STATUS ,
  COALESCE(OVERDUE_DAYS, 0 ) AS OVERDUE_DAYS ,
  COALESCE(CANCEL_DATE,DATE('4999-12-31') ) AS CANCEL_DATE ,
  COALESCE(SPAREINTERESTFALG, '' ) AS SPAREINTERESTFALG ,
  COALESCE(OVERDUE_AMOUNT, 0 ) AS OVERDUE_AMOUNT ,
  COALESCE(OVERDUE_RATE_AMOUNT, 0 ) AS OVERDUE_RATE_AMOUNT ,
  COALESCE(OVERDUE_TOTAL, 0 ) AS OVERDUE_TOTAL ,
  COALESCE(CLOSE_DATE,DATE('4999-12-31') ) AS CLOSE_DATE ,
  COALESCE(LOCK_FLAG, '' ) AS LOCK_FLAG ,
  COALESCE(IS_STOP_REPAYMENT, '' ) AS IS_STOP_REPAYMENT ,
  COALESCE(BIZ_CHANNEL_KIND, '' ) AS BIZ_CHANNEL_KIND ,
  COALESCE(BIZ_CHANNEL_SUB_KIND, '' ) AS BIZ_CHANNEL_SUB_KIND ,
  COALESCE(GSYS_CENTER_NUM, '' ) AS GSYS_CENTER_NUM ,
  COALESCE(GSYS_BANK_ORG, '' ) AS GSYS_BANK_ORG ,
  COALESCE(GSYS_DUEBILL_NO, '' ) AS GSYS_DUEBILL_NO, 
  COALESCE(RECORD_DATE,DATE('4999-12-31')) AS RECORD_DATE
 FROM  dw_tdata.PCS_006_TB_LON_LOAN_DUEBILL_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  LOAN_ID ,
  LOAN_CONTRACT_ID ,
  FLOW_NO ,
  DUEBILL_ID ,
  DUEBILL_NO ,
  DUEBILL_AMOUNT ,
  DUEBILL_BALANCE ,
  DUEBILL_INTEREST_BALANCE ,
  DUEBILL_RATE ,
  CURRENCY ,
  NOTICE_NO ,
  DUEBILL_BEGIN_DATE ,
  DUEBILL_END_DATE ,
  ACCOUNT_NO ,
  EXTENSION_FLAG ,
  EXTENSION_DATE ,
  DUEBILL_STATUS ,
  LOAN_LEVEL_FIVE_CLASS ,
  DUEBILL_BALANCE_NORMAL ,
  DUEBILL_BALANCE_OVERDUE ,
  DUEBILL_BALANCE_DULL ,
  DUEBILL_BALANCE_BAD ,
  CANCEL_CAPITAL_BALANCE ,
  CANCEL_IN_SHEET_BALANCE ,
  CANCEL_OFF_SHEET_BALANCE ,
  IN_SHEET_INTEREST_BALANCE ,
  OFF_SHEET_INTEREST_BALANCE ,
  INTEREST_INCOME ,
  COUNT_OVERDUE_TERM ,
  CURRENT_OVERDUE_TERM ,
  MAX_OVERDUE_TERM ,
  ALLOW_MAX_OVERDUE_DAY ,
  CLOSE_FLAG ,
  OLD_DUEBILL_ID ,
  OLD_DUEBILL_NO ,
  PAYEE_NO ,
  STOP_PAY_NO ,
  REPAY_NO ,
  LOAN_STOP_PAY_NO ,
  TRANS_FLOW_NO ,
  CHECK_DATE ,
  LOCAL_DATE ,
  LOCAL_TIME ,
  IS_STOP_INTEREST_FLAG ,
  IS_DISCOUNT ,
  BEGIN_REPAY_CAPITAL_MONTH ,
  IS_LAW_STOP_INTEREST_FLAG ,
  IS_CARD_LOAN ,
  IS_CARD_REPAY ,
  IS_CARD_TRANSFER ,
  ACCOUNT_NAME ,
  REPAY_NAME ,
  PAYEE_NAME ,
  NORMAL_IN_SHEET_INTEREST ,
  NORMAL_OFF_SHEET_INTEREST ,
  OVERDUE_IN_SHEET_INTEREST ,
  OVERDUE_OFF_SHEET_INTEREST ,
  DEFAULT_IN_SHEET_INTEREST ,
  DEFAULT_OFF_SHEET_INTEREST ,
  CANCEL_CAPITAL ,
  CANCEL_INTEREST ,
  CAPITAL_AMOUNT ,
  END_SUM_AMOUNT ,
  DEBT_SUM_EVALUATE ,
  CANCEL_ACCRUED_INTEREST ,
  PROJECT_ID ,
  FRANK ,
  ENTRUST_PAYMENT_STATUS ,
  PAYMENT_WAY ,
  OVERDUE_RATE ,
  PROVINCE_NUM ,
  NORMAL_PAYMENT_AMOUNT ,
  ENTRUST_PAYMENT_AMOUNT ,
  SPECIAL_PAYMENT_AMOUNT ,
  DELFLAG ,
  CREATE_TIME ,
  UPDATE_TIME ,
  TRUNC_NO ,
  EFF_CURR_PERI ,
  DISCOUNT_STATUS ,
  OVERDUE_DAYS ,
  CANCEL_DATE ,
  SPAREINTERESTFALG ,
  OVERDUE_AMOUNT ,
  OVERDUE_RATE_AMOUNT ,
  OVERDUE_TOTAL ,
  CLOSE_DATE ,
  LOCK_FLAG ,
  IS_STOP_REPAYMENT ,
  BIZ_CHANNEL_KIND ,
  BIZ_CHANNEL_SUB_KIND ,
  GSYS_CENTER_NUM ,
  GSYS_BANK_ORG ,
  GSYS_DUEBILL_NO ,
  RECORD_DATE
 FROM dw_sdata.PCS_006_TB_LON_LOAN_DUEBILL 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.DUEBILL_ID = T.DUEBILL_ID
WHERE
(T.DUEBILL_ID IS NULL)
 OR N.LOAN_ID<>T.LOAN_ID
 OR N.LOAN_CONTRACT_ID<>T.LOAN_CONTRACT_ID
 OR N.FLOW_NO<>T.FLOW_NO
 OR N.DUEBILL_NO<>T.DUEBILL_NO
 OR N.DUEBILL_AMOUNT<>T.DUEBILL_AMOUNT
 OR N.DUEBILL_BALANCE<>T.DUEBILL_BALANCE
 OR N.DUEBILL_INTEREST_BALANCE<>T.DUEBILL_INTEREST_BALANCE
 OR N.DUEBILL_RATE<>T.DUEBILL_RATE
 OR N.CURRENCY<>T.CURRENCY
 OR N.NOTICE_NO<>T.NOTICE_NO
 OR N.DUEBILL_BEGIN_DATE<>T.DUEBILL_BEGIN_DATE
 OR N.DUEBILL_END_DATE<>T.DUEBILL_END_DATE
 OR N.ACCOUNT_NO<>T.ACCOUNT_NO
 OR N.EXTENSION_FLAG<>T.EXTENSION_FLAG
 OR N.EXTENSION_DATE<>T.EXTENSION_DATE
 OR N.DUEBILL_STATUS<>T.DUEBILL_STATUS
 OR N.LOAN_LEVEL_FIVE_CLASS<>T.LOAN_LEVEL_FIVE_CLASS
 OR N.DUEBILL_BALANCE_NORMAL<>T.DUEBILL_BALANCE_NORMAL
 OR N.DUEBILL_BALANCE_OVERDUE<>T.DUEBILL_BALANCE_OVERDUE
 OR N.DUEBILL_BALANCE_DULL<>T.DUEBILL_BALANCE_DULL
 OR N.DUEBILL_BALANCE_BAD<>T.DUEBILL_BALANCE_BAD
 OR N.CANCEL_CAPITAL_BALANCE<>T.CANCEL_CAPITAL_BALANCE
 OR N.CANCEL_IN_SHEET_BALANCE<>T.CANCEL_IN_SHEET_BALANCE
 OR N.CANCEL_OFF_SHEET_BALANCE<>T.CANCEL_OFF_SHEET_BALANCE
 OR N.IN_SHEET_INTEREST_BALANCE<>T.IN_SHEET_INTEREST_BALANCE
 OR N.OFF_SHEET_INTEREST_BALANCE<>T.OFF_SHEET_INTEREST_BALANCE
 OR N.INTEREST_INCOME<>T.INTEREST_INCOME
 OR N.COUNT_OVERDUE_TERM<>T.COUNT_OVERDUE_TERM
 OR N.CURRENT_OVERDUE_TERM<>T.CURRENT_OVERDUE_TERM
 OR N.MAX_OVERDUE_TERM<>T.MAX_OVERDUE_TERM
 OR N.ALLOW_MAX_OVERDUE_DAY<>T.ALLOW_MAX_OVERDUE_DAY
 OR N.CLOSE_FLAG<>T.CLOSE_FLAG
 OR N.OLD_DUEBILL_ID<>T.OLD_DUEBILL_ID
 OR N.OLD_DUEBILL_NO<>T.OLD_DUEBILL_NO
 OR N.PAYEE_NO<>T.PAYEE_NO
 OR N.STOP_PAY_NO<>T.STOP_PAY_NO
 OR N.REPAY_NO<>T.REPAY_NO
 OR N.LOAN_STOP_PAY_NO<>T.LOAN_STOP_PAY_NO
 OR N.TRANS_FLOW_NO<>T.TRANS_FLOW_NO
 OR N.CHECK_DATE<>T.CHECK_DATE
 OR N.LOCAL_DATE<>T.LOCAL_DATE
 OR N.LOCAL_TIME<>T.LOCAL_TIME
 OR N.IS_STOP_INTEREST_FLAG<>T.IS_STOP_INTEREST_FLAG
 OR N.IS_DISCOUNT<>T.IS_DISCOUNT
 OR N.BEGIN_REPAY_CAPITAL_MONTH<>T.BEGIN_REPAY_CAPITAL_MONTH
 OR N.IS_LAW_STOP_INTEREST_FLAG<>T.IS_LAW_STOP_INTEREST_FLAG
 OR N.IS_CARD_LOAN<>T.IS_CARD_LOAN
 OR N.IS_CARD_REPAY<>T.IS_CARD_REPAY
 OR N.IS_CARD_TRANSFER<>T.IS_CARD_TRANSFER
 OR N.ACCOUNT_NAME<>T.ACCOUNT_NAME
 OR N.REPAY_NAME<>T.REPAY_NAME
 OR N.PAYEE_NAME<>T.PAYEE_NAME
 OR N.NORMAL_IN_SHEET_INTEREST<>T.NORMAL_IN_SHEET_INTEREST
 OR N.NORMAL_OFF_SHEET_INTEREST<>T.NORMAL_OFF_SHEET_INTEREST
 OR N.OVERDUE_IN_SHEET_INTEREST<>T.OVERDUE_IN_SHEET_INTEREST
 OR N.OVERDUE_OFF_SHEET_INTEREST<>T.OVERDUE_OFF_SHEET_INTEREST
 OR N.DEFAULT_IN_SHEET_INTEREST<>T.DEFAULT_IN_SHEET_INTEREST
 OR N.DEFAULT_OFF_SHEET_INTEREST<>T.DEFAULT_OFF_SHEET_INTEREST
 OR N.CANCEL_CAPITAL<>T.CANCEL_CAPITAL
 OR N.CANCEL_INTEREST<>T.CANCEL_INTEREST
 OR N.CAPITAL_AMOUNT<>T.CAPITAL_AMOUNT
 OR N.END_SUM_AMOUNT<>T.END_SUM_AMOUNT
 OR N.DEBT_SUM_EVALUATE<>T.DEBT_SUM_EVALUATE
 OR N.CANCEL_ACCRUED_INTEREST<>T.CANCEL_ACCRUED_INTEREST
 OR N.PROJECT_ID<>T.PROJECT_ID
 OR N.FRANK<>T.FRANK
 OR N.ENTRUST_PAYMENT_STATUS<>T.ENTRUST_PAYMENT_STATUS
 OR N.PAYMENT_WAY<>T.PAYMENT_WAY
 OR N.OVERDUE_RATE<>T.OVERDUE_RATE
 OR N.PROVINCE_NUM<>T.PROVINCE_NUM
 OR N.NORMAL_PAYMENT_AMOUNT<>T.NORMAL_PAYMENT_AMOUNT
 OR N.ENTRUST_PAYMENT_AMOUNT<>T.ENTRUST_PAYMENT_AMOUNT
 OR N.SPECIAL_PAYMENT_AMOUNT<>T.SPECIAL_PAYMENT_AMOUNT
 OR N.DELFLAG<>T.DELFLAG
 OR N.CREATE_TIME<>T.CREATE_TIME
 OR N.UPDATE_TIME<>T.UPDATE_TIME
 OR N.TRUNC_NO<>T.TRUNC_NO
 OR N.EFF_CURR_PERI<>T.EFF_CURR_PERI
 OR N.DISCOUNT_STATUS<>T.DISCOUNT_STATUS
 OR N.OVERDUE_DAYS<>T.OVERDUE_DAYS
 OR N.CANCEL_DATE<>T.CANCEL_DATE
 OR N.SPAREINTERESTFALG<>T.SPAREINTERESTFALG
 OR N.OVERDUE_AMOUNT<>T.OVERDUE_AMOUNT
 OR N.OVERDUE_RATE_AMOUNT<>T.OVERDUE_RATE_AMOUNT
 OR N.OVERDUE_TOTAL<>T.OVERDUE_TOTAL
 OR N.CLOSE_DATE<>T.CLOSE_DATE
 OR N.LOCK_FLAG<>T.LOCK_FLAG
 OR N.IS_STOP_REPAYMENT<>T.IS_STOP_REPAYMENT
 OR N.BIZ_CHANNEL_KIND<>T.BIZ_CHANNEL_KIND
 OR N.BIZ_CHANNEL_SUB_KIND<>T.BIZ_CHANNEL_SUB_KIND
 OR N.GSYS_CENTER_NUM<>T.GSYS_CENTER_NUM
 OR N.GSYS_BANK_ORG<>T.GSYS_BANK_ORG
 OR N.GSYS_DUEBILL_NO<>T.GSYS_DUEBILL_NO
 OR N.RECORD_DATE<>T.RECORD_DATE
;

--Step3:
UPDATE dw_sdata.PCS_006_TB_LON_LOAN_DUEBILL P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_338
WHERE P.End_Dt=DATE('2100-12-31')
AND P.DUEBILL_ID=T_338.DUEBILL_ID
;

--Step4:
INSERT  INTO dw_sdata.PCS_006_TB_LON_LOAN_DUEBILL SELECT * FROM T_338;

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
