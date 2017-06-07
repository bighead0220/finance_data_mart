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

DELETE FROM dw_sdata.CBS_001_AMDTL_CORP WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.CBS_001_AMDTL_CORP(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  INTER_ACCT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLR_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CEN_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SEQ_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  UNIT_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TERMI_MARK ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLI_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHNL_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TIME_STAMP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INT_START_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPEN_UNIT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CUST_ACCT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUBACCT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  VIRACCT_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_TIME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DAYS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BAL_WEIT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ABST_PAGE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ABST_NUM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  VCHR_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  VCHR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BRIEF_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BRIEF ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OUT_IN_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AMOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DC_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CASH_TRSF_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CUR_BAL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BAL_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPS_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPS_SUBACCT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPS_VIRACCT_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPS_NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPS_BANK_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPS_AREA_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ABSTR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_INFO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPERATOR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHECKER ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AUTH_OPR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IF_TD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  VALID_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  QX_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FUND_GATHER ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPS_BANK_NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(INTER_ACCT, 0 ) AS INTER_ACCT ,
  COALESCE(CLR_DATE, '' ) AS CLR_DATE ,
  COALESCE(CEN_SERIAL_NO, 0 ) AS CEN_SERIAL_NO ,
  COALESCE(SEQ_NO, 0 ) AS SEQ_NO ,
  COALESCE(TX_DATE, '' ) AS TX_DATE ,
  COALESCE(UNIT_CODE, '' ) AS UNIT_CODE ,
  COALESCE(TERMI_MARK, '' ) AS TERMI_MARK ,
  COALESCE(CLI_SERIAL_NO, '' ) AS CLI_SERIAL_NO ,
  COALESCE(CHNL_CODE, '' ) AS CHNL_CODE ,
  COALESCE(TX_CODE, '' ) AS TX_CODE ,
  COALESCE(TIME_STAMP, '' ) AS TIME_STAMP ,
  COALESCE(INT_START_DATE, '' ) AS INT_START_DATE ,
  COALESCE(OPEN_UNIT, '' ) AS OPEN_UNIT ,
  COALESCE(CUST_ACCT, '' ) AS CUST_ACCT ,
  COALESCE(SUBACCT, 0 ) AS SUBACCT ,
  COALESCE(VIRACCT_NO, '' ) AS VIRACCT_NO ,
  COALESCE(TX_TIME, '' ) AS TX_TIME ,
  COALESCE(DAYS, 0 ) AS DAYS ,
  COALESCE(BAL_WEIT, 0 ) AS BAL_WEIT ,
  COALESCE(ABST_PAGE, 0 ) AS ABST_PAGE ,
  COALESCE(ABST_NUM, 0 ) AS ABST_NUM ,
  COALESCE(VCHR_NO, '' ) AS VCHR_NO ,
  COALESCE(VCHR_TYPE, '' ) AS VCHR_TYPE ,
  COALESCE(BRIEF_CODE, 0 ) AS BRIEF_CODE ,
  COALESCE(BRIEF, '' ) AS BRIEF ,
  COALESCE(TX_TYPE, '' ) AS TX_TYPE ,
  COALESCE(OUT_IN_FLAG, '' ) AS OUT_IN_FLAG ,
  COALESCE(AMOUNT, 0 ) AS AMOUNT ,
  COALESCE(DC_FLAG, '' ) AS DC_FLAG ,
  COALESCE(CASH_TRSF_FLAG, '' ) AS CASH_TRSF_FLAG ,
  COALESCE(CUR_BAL, 0 ) AS CUR_BAL ,
  COALESCE(BAL_FLAG, '' ) AS BAL_FLAG ,
  COALESCE(OPS_ACCOUNT, '' ) AS OPS_ACCOUNT ,
  COALESCE(OPS_SUBACCT, 0 ) AS OPS_SUBACCT ,
  COALESCE(OPS_VIRACCT_NO, '' ) AS OPS_VIRACCT_NO ,
  COALESCE(OPS_NAME, '' ) AS OPS_NAME ,
  COALESCE(OPS_BANK_NO, '' ) AS OPS_BANK_NO ,
  COALESCE(OPS_AREA_CODE, '' ) AS OPS_AREA_CODE ,
  COALESCE(ABSTR, '' ) AS ABSTR ,
  COALESCE(TX_INFO, '' ) AS TX_INFO ,
  COALESCE(OPERATOR, '' ) AS OPERATOR ,
  COALESCE(CHECKER, '' ) AS CHECKER ,
  COALESCE(AUTH_OPR, '' ) AS AUTH_OPR ,
  COALESCE(IF_TD, '' ) AS IF_TD ,
  COALESCE(VALID_FLAG, '' ) AS VALID_FLAG ,
  COALESCE(QX_FLAG, '' ) AS QX_FLAG ,
  COALESCE(FUND_GATHER, '' ) AS FUND_GATHER ,
  COALESCE(OPS_BANK_NAME, '' ) AS OPS_BANK_NAME ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.CBS_001_AMDTL_CORP_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
