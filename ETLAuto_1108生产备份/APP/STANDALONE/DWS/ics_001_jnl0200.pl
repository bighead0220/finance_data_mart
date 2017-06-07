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

DELETE FROM dw_sdata.ICS_001_JNL WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.ICS_001_JNL(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  CLR_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CEN_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  UNIT_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TERMI_MARK ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLI_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHNL_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TIME_STAMP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SETTLE_UNIT_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ADD_UNIT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CASH_BOX_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACCT_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INSU_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INSU_PRO_KIND ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INSU_KIND_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAY_METHOD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHARGE_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHARGE_YEAR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  THIS_PAY_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  COVERAGE_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  COVERAGE_YEAR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  APPF_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  POH_NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  POL_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAYMT_MODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PERM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUM_INSURED ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CARD_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SAV_ACCT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  VCH_SIGN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  VCH_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  VCH_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CSIC_JNL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CSIC_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RECV_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ZONE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BANK_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BAT_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TPIC_PAY_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TPIC_PAY_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TPIC_PAY_STATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  VALID_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPERATOR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHECKER ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AUTH_OPR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AUTH_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_STATE_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RET_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  LOG_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ABNORM_TIME_STAMP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ABNORM_RET_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ABNORM_LOG_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORG_CLR_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORG_CEN_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SVC_NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BUFFER ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(CLR_DATE, '' ) AS CLR_DATE ,
  COALESCE(CEN_SERIAL_NO, 0 ) AS CEN_SERIAL_NO ,
  COALESCE(TX_DATE, '' ) AS TX_DATE ,
  COALESCE(UNIT_CODE, '' ) AS UNIT_CODE ,
  COALESCE(TERMI_MARK, '' ) AS TERMI_MARK ,
  COALESCE(CLI_SERIAL_NO, 0 ) AS CLI_SERIAL_NO ,
  COALESCE(CHNL_CODE, '' ) AS CHNL_CODE ,
  COALESCE(TX_CODE, '' ) AS TX_CODE ,
  COALESCE(TIME_STAMP, '' ) AS TIME_STAMP ,
  COALESCE(SETTLE_UNIT_CODE, '' ) AS SETTLE_UNIT_CODE ,
  COALESCE(ADD_UNIT, '' ) AS ADD_UNIT ,
  COALESCE(CASH_BOX_NO, '' ) AS CASH_BOX_NO ,
  COALESCE(ACCT_TYPE, '' ) AS ACCT_TYPE ,
  COALESCE(TX_AMT, 0 ) AS TX_AMT ,
  COALESCE(INSU_CODE, '' ) AS INSU_CODE ,
  COALESCE(INSU_PRO_KIND, '' ) AS INSU_PRO_KIND ,
  COALESCE(INSU_KIND_CODE, '' ) AS INSU_KIND_CODE ,
  COALESCE(PAY_METHOD, '' ) AS PAY_METHOD ,
  COALESCE(CHARGE_TYPE, '' ) AS CHARGE_TYPE ,
  COALESCE(CHARGE_YEAR, 0 ) AS CHARGE_YEAR ,
  COALESCE(THIS_PAY_NO, 0 ) AS THIS_PAY_NO ,
  COALESCE(COVERAGE_TYPE, '' ) AS COVERAGE_TYPE ,
  COALESCE(COVERAGE_YEAR, 0 ) AS COVERAGE_YEAR ,
  COALESCE(APPF_NO, '' ) AS APPF_NO ,
  COALESCE(POH_NAME, '' ) AS POH_NAME ,
  COALESCE(POL_ID, '' ) AS POL_ID ,
  COALESCE(PAYMT_MODE, '' ) AS PAYMT_MODE ,
  COALESCE(PERM, 0 ) AS PERM ,
  COALESCE(SUM_INSURED, 0 ) AS SUM_INSURED ,
  COALESCE(CARD_FLAG, '' ) AS CARD_FLAG ,
  COALESCE(SAV_ACCT, '' ) AS SAV_ACCT ,
  COALESCE(VCH_SIGN, '' ) AS VCH_SIGN ,
  COALESCE(VCH_TYPE, '' ) AS VCH_TYPE ,
  COALESCE(VCH_ID, '' ) AS VCH_ID ,
  COALESCE(CSIC_JNL_NO, '' ) AS CSIC_JNL_NO ,
  COALESCE(CSIC_DATE, '' ) AS CSIC_DATE ,
  COALESCE(RECV_DATE, '' ) AS RECV_DATE ,
  COALESCE(ZONE, '' ) AS ZONE ,
  COALESCE(BANK_CODE, '' ) AS BANK_CODE ,
  COALESCE(BAT_NO, '' ) AS BAT_NO ,
  COALESCE(TPIC_PAY_ID, '' ) AS TPIC_PAY_ID ,
  COALESCE(TPIC_PAY_AMT, 0 ) AS TPIC_PAY_AMT ,
  COALESCE(TPIC_PAY_STATE, '' ) AS TPIC_PAY_STATE ,
  COALESCE(VALID_FLAG, '' ) AS VALID_FLAG ,
  COALESCE(OPERATOR, '' ) AS OPERATOR ,
  COALESCE(CHECKER, '' ) AS CHECKER ,
  COALESCE(AUTH_OPR, '' ) AS AUTH_OPR ,
  COALESCE(AUTH_CODE, '' ) AS AUTH_CODE ,
  COALESCE(TX_STATE_CODE, '' ) AS TX_STATE_CODE ,
  COALESCE(RET_CODE, '' ) AS RET_CODE ,
  COALESCE(LOG_ID, '' ) AS LOG_ID ,
  COALESCE(ABNORM_TIME_STAMP, '' ) AS ABNORM_TIME_STAMP ,
  COALESCE(ABNORM_RET_CODE, '' ) AS ABNORM_RET_CODE ,
  COALESCE(ABNORM_LOG_ID, '' ) AS ABNORM_LOG_ID ,
  COALESCE(ORG_CLR_DATE, '' ) AS ORG_CLR_DATE ,
  COALESCE(ORG_CEN_SERIAL_NO, 0 ) AS ORG_CEN_SERIAL_NO ,
  COALESCE(SVC_NAME, '' ) AS SVC_NAME ,
  COALESCE(BUFFER, '' ) AS BUFFER ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.ICS_001_JNL_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
