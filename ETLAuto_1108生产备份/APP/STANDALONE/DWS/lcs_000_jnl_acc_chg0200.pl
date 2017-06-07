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

DELETE FROM dw_sdata.LCS_000_JNL_ACC_CHG WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.LCS_000_JNL_ACC_CHG(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  CEN_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLI_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLR_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BGN_INT_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TIME_STAMP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_UNIT_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPERATOR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_NAMECODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_CHARA ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_STATE_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DAC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_INST_ROLE_NUM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_INST_ROLE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_AGENCY_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_ACCOUNT_SYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IDEN_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IDEN_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUB_ACC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BILL_GROUPQTY ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BILL_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BILL_USE_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  VOUCHER_START_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  VOUCHER_END_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  VOUCHER_COUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_GROUPQTY ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AMT_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CURR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INT_TX_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INT_TX_ACCOUNT_SYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INT_MATCH_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INT_MATCH_ACCOUNT_SYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CASH_USEMODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_GROUPQTY ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_TX_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_TX_ACCOUNT_SYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_TX_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_CURR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  HAND_FEE_MODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PREF_FEE_UNIT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PREF_FEE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_PROV_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_COLL_UNIT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SYS_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BGN_TX_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OUTSYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INSYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  MERCHANT_MARK ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TERMI_MARK ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SHOP_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORG_JNL_INFO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  EXTEND_INFO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAPER_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAPER_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AGT_NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AGT_PAPER_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AGT_PAPER_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AGT_PHONE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TSFIN_BANKCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TSFIN_BANKNAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TSFIN_NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TSFOUT_BANKCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TSFOUT_BANKNAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TSFOUT_NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AUTH_TRL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INN_TRAN_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORIG_DATE1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORIG_CLT_SEQNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AMT_DR_CR_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OTHBANK_CARD_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FUNDS_STAT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(CEN_SERIAL_NO, '' ) AS CEN_SERIAL_NO ,
  COALESCE(CLI_SERIAL_NO, '' ) AS CLI_SERIAL_NO ,
  COALESCE(CLR_DATE, '' ) AS CLR_DATE ,
  COALESCE(BGN_INT_DATE, '' ) AS BGN_INT_DATE ,
  COALESCE(TIME_STAMP, '' ) AS TIME_STAMP ,
  COALESCE(SYS_CODE, '' ) AS SYS_CODE ,
  COALESCE(TX_UNIT_CODE, '' ) AS TX_UNIT_CODE ,
  COALESCE(OPERATOR, '' ) AS OPERATOR ,
  COALESCE(TX_NAMECODE, '' ) AS TX_NAMECODE ,
  COALESCE(TX_CHARA, '' ) AS TX_CHARA ,
  COALESCE(TX_STATE_CODE, '' ) AS TX_STATE_CODE ,
  COALESCE(DAC, '' ) AS DAC ,
  COALESCE(TX_INST_ROLE_NUM, 0 ) AS TX_INST_ROLE_NUM ,
  COALESCE(TX_INST_ROLE, '' ) AS TX_INST_ROLE ,
  COALESCE(TX_AGENCY_CODE, '' ) AS TX_AGENCY_CODE ,
  COALESCE(TX_ACCOUNT, '' ) AS TX_ACCOUNT ,
  COALESCE(TX_ACCOUNT_SYS_CODE, '' ) AS TX_ACCOUNT_SYS_CODE ,
  COALESCE(IDEN_NO, '' ) AS IDEN_NO ,
  COALESCE(IDEN_TYPE, '' ) AS IDEN_TYPE ,
  COALESCE(SUB_ACC, '' ) AS SUB_ACC ,
  COALESCE(BILL_GROUPQTY, 0 ) AS BILL_GROUPQTY ,
  COALESCE(BILL_TYPE, '' ) AS BILL_TYPE ,
  COALESCE(BILL_USE_TYPE, '' ) AS BILL_USE_TYPE ,
  COALESCE(VOUCHER_START_NO, '' ) AS VOUCHER_START_NO ,
  COALESCE(VOUCHER_END_NO, '' ) AS VOUCHER_END_NO ,
  COALESCE(VOUCHER_COUNT, '' ) AS VOUCHER_COUNT ,
  COALESCE(TX_GROUPQTY, 0 ) AS TX_GROUPQTY ,
  COALESCE(AMT_TYPE, '' ) AS AMT_TYPE ,
  COALESCE(TX_AMT, '' ) AS TX_AMT ,
  COALESCE(CURR_TYPE, '' ) AS CURR_TYPE ,
  COALESCE(INT_TX_ACCOUNT, '' ) AS INT_TX_ACCOUNT ,
  COALESCE(INT_TX_ACCOUNT_SYS_CODE, '' ) AS INT_TX_ACCOUNT_SYS_CODE ,
  COALESCE(INT_MATCH_ACCOUNT, '' ) AS INT_MATCH_ACCOUNT ,
  COALESCE(INT_MATCH_ACCOUNT_SYS_CODE, '' ) AS INT_MATCH_ACCOUNT_SYS_CODE ,
  COALESCE(CASH_USEMODE, '' ) AS CASH_USEMODE ,
  COALESCE(FEE_GROUPQTY, 0 ) AS FEE_GROUPQTY ,
  COALESCE(FEE_TX_ACCOUNT, '' ) AS FEE_TX_ACCOUNT ,
  COALESCE(FEE_TX_ACCOUNT_SYS_CODE, '' ) AS FEE_TX_ACCOUNT_SYS_CODE ,
  COALESCE(FEE_TX_AMT, '' ) AS FEE_TX_AMT ,
  COALESCE(FEE_TYPE, '' ) AS FEE_TYPE ,
  COALESCE(FEE_CURR_TYPE, '' ) AS FEE_CURR_TYPE ,
  COALESCE(HAND_FEE_MODE, '' ) AS HAND_FEE_MODE ,
  COALESCE(PREF_FEE_UNIT, '' ) AS PREF_FEE_UNIT ,
  COALESCE(PREF_FEE, '' ) AS PREF_FEE ,
  COALESCE(FEE_PROV_FLAG, '' ) AS FEE_PROV_FLAG ,
  COALESCE(FEE_COLL_UNIT, '' ) AS FEE_COLL_UNIT ,
  COALESCE(SYS_SERIAL_NO, '' ) AS SYS_SERIAL_NO ,
  COALESCE(BGN_TX_DATE, '' ) AS BGN_TX_DATE ,
  COALESCE(OUTSYS_CODE, '' ) AS OUTSYS_CODE ,
  COALESCE(INSYS_CODE, '' ) AS INSYS_CODE ,
  COALESCE(MERCHANT_MARK, '' ) AS MERCHANT_MARK ,
  COALESCE(TERMI_MARK, '' ) AS TERMI_MARK ,
  COALESCE(SHOP_TYPE, '' ) AS SHOP_TYPE ,
  COALESCE(ORG_JNL_INFO, '' ) AS ORG_JNL_INFO ,
  COALESCE(EXTEND_INFO, '' ) AS EXTEND_INFO ,
  COALESCE(PAPER_NO, '' ) AS PAPER_NO ,
  COALESCE(PAPER_TYPE, '' ) AS PAPER_TYPE ,
  COALESCE(AGT_NAME, '' ) AS AGT_NAME ,
  COALESCE(AGT_PAPER_NO, '' ) AS AGT_PAPER_NO ,
  COALESCE(AGT_PAPER_TYPE, '' ) AS AGT_PAPER_TYPE ,
  COALESCE(AGT_PHONE, '' ) AS AGT_PHONE ,
  COALESCE(TSFIN_BANKCODE, '' ) AS TSFIN_BANKCODE ,
  COALESCE(TSFIN_BANKNAME, '' ) AS TSFIN_BANKNAME ,
  COALESCE(TSFIN_NAME, '' ) AS TSFIN_NAME ,
  COALESCE(TSFOUT_BANKCODE, '' ) AS TSFOUT_BANKCODE ,
  COALESCE(TSFOUT_BANKNAME, '' ) AS TSFOUT_BANKNAME ,
  COALESCE(TSFOUT_NAME, '' ) AS TSFOUT_NAME ,
  COALESCE(AUTH_TRL_NO, '' ) AS AUTH_TRL_NO ,
  COALESCE(INN_TRAN_NO, '' ) AS INN_TRAN_NO ,
  COALESCE(ORIG_DATE1, '' ) AS ORIG_DATE1 ,
  COALESCE(ORIG_CLT_SEQNO, '' ) AS ORIG_CLT_SEQNO ,
  COALESCE(AMT_DR_CR_FLAG, '' ) AS AMT_DR_CR_FLAG ,
  COALESCE(OTHBANK_CARD_NO, '' ) AS OTHBANK_CARD_NO ,
  COALESCE(FUNDS_STAT, '' ) AS FUNDS_STAT ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.LCS_000_JNL_ACC_CHG_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
;                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
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
