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

DELETE FROM dw_sdata.CBS_001_JNL WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.CBS_001_JNL(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  CLR_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CEN_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  UNIT_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TERMI_MARK ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLI_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SERIAL_SEQNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHNL_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TIME_STAMP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TI_TILL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CUR_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DC_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPEN_UNIT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUBACCT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DEP_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPS_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPS_OPEN_UNIT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPS_SUBACCT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPS_DEP_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  VCHR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  VCHR_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  WRITE_OFF_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BRIEF_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BRIEF ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INT_BEGIN_DAY ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SEAL_CARD_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAPER_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PAPER_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  HANDLE_PAPER_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  HANDLE_PAPER_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_FEE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUCC_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPERATOR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHECKER ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AUTH_OPR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AUTH_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_STATE_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ABNORM_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RET_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  LOG_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORG_CLR_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORG_CEN_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SVC_NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUBSYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BUFFER ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(CLR_DATE, '' ) AS CLR_DATE ,
  COALESCE(CEN_SERIAL_NO, 0 ) AS CEN_SERIAL_NO ,
  COALESCE(TX_DATE, '' ) AS TX_DATE ,
  COALESCE(UNIT_CODE, '' ) AS UNIT_CODE ,
  COALESCE(TERMI_MARK, '' ) AS TERMI_MARK ,
  COALESCE(CLI_SERIAL_NO, '' ) AS CLI_SERIAL_NO ,
  COALESCE(SERIAL_SEQNO, 0 ) AS SERIAL_SEQNO ,
  COALESCE(CHNL_CODE, '' ) AS CHNL_CODE ,
  COALESCE(TX_CODE, '' ) AS TX_CODE ,
  COALESCE(TIME_STAMP, '' ) AS TIME_STAMP ,
  COALESCE(TI_TILL_NO, '' ) AS TI_TILL_NO ,
  COALESCE(CUR_CODE, '' ) AS CUR_CODE ,
  COALESCE(TX_AMT, 0 ) AS TX_AMT ,
  COALESCE(DC_FLAG, '' ) AS DC_FLAG ,
  COALESCE(ACCOUNT, '' ) AS ACCOUNT ,
  COALESCE(OPEN_UNIT, '' ) AS OPEN_UNIT ,
  COALESCE(SUBACCT, 0 ) AS SUBACCT ,
  COALESCE(DEP_TYPE, '' ) AS DEP_TYPE ,
  COALESCE(OPS_ACCOUNT, '' ) AS OPS_ACCOUNT ,
  COALESCE(OPS_OPEN_UNIT, '' ) AS OPS_OPEN_UNIT ,
  COALESCE(OPS_SUBACCT, 0 ) AS OPS_SUBACCT ,
  COALESCE(OPS_DEP_TYPE, '' ) AS OPS_DEP_TYPE ,
  COALESCE(VCHR_TYPE, '' ) AS VCHR_TYPE ,
  COALESCE(VCHR_NO, '' ) AS VCHR_NO ,
  COALESCE(WRITE_OFF_NO, '' ) AS WRITE_OFF_NO ,
  COALESCE(BRIEF_CODE, 0 ) AS BRIEF_CODE ,
  COALESCE(BRIEF, '' ) AS BRIEF ,
  COALESCE(INT_BEGIN_DAY, '' ) AS INT_BEGIN_DAY ,
  COALESCE(SEAL_CARD_NO, '' ) AS SEAL_CARD_NO ,
  COALESCE(PAPER_TYPE, '' ) AS PAPER_TYPE ,
  COALESCE(PAPER_NO, '' ) AS PAPER_NO ,
  COALESCE(HANDLE_PAPER_TYPE, '' ) AS HANDLE_PAPER_TYPE ,
  COALESCE(HANDLE_PAPER_NO, '' ) AS HANDLE_PAPER_NO ,
  COALESCE(TX_FEE, 0 ) AS TX_FEE ,
  COALESCE(SUCC_FLAG, '' ) AS SUCC_FLAG ,
  COALESCE(OPERATOR, '' ) AS OPERATOR ,
  COALESCE(CHECKER, '' ) AS CHECKER ,
  COALESCE(AUTH_OPR, '' ) AS AUTH_OPR ,
  COALESCE(AUTH_CODE, '' ) AS AUTH_CODE ,
  COALESCE(TX_STATE_CODE, '' ) AS TX_STATE_CODE ,
  COALESCE(ABNORM_FLAG, '' ) AS ABNORM_FLAG ,
  COALESCE(RET_CODE, '' ) AS RET_CODE ,
  COALESCE(LOG_ID, '' ) AS LOG_ID ,
  COALESCE(ORG_CLR_DATE, '' ) AS ORG_CLR_DATE ,
  COALESCE(ORG_CEN_SERIAL_NO, 0 ) AS ORG_CEN_SERIAL_NO ,
  COALESCE(SVC_NAME, '' ) AS SVC_NAME ,
  COALESCE(SUBSYS_CODE, '' ) AS SUBSYS_CODE ,
  COALESCE(BUFFER, '' ) AS BUFFER ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.CBS_001_JNL_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
