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

DELETE FROM dw_sdata.BBS_001_SUB_MSG_TOGETHER WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.BBS_001_SUB_MSG_TOGETHER(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  MAIN_MSG_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CEN_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLI_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLR_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BGN_INT_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TIME_STAMP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_UNIT_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPERATOR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_CHARA ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_STATE_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OUTSYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INSYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BGN_TX_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORG_JNL_INFO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  EXTEND_INFO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_NAMECODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  O_TX_INST_ROLE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  O_TX_AGENCY_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  O_TX_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  O_TX_ACCOUNT_SYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  T_TX_INST_ROLE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  T_TX_AGENCY_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  T_TX_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  T_TX_ACCOUNT_SYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BILL_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  VOUCHER_COUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IMP_BILL_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  A_TX_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  A_AMT_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  A_CURR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  A_INT_TX_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  B_TX_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  B_AMT_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  B_CURR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  B_INT_TX_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  C_TX_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  C_AMT_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  C_CURR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  C_INT_TX_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  D_TX_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  D_AMT_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  D_CURR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  D_INT_TX_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  E_TX_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  E_AMT_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  E_CURR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  E_INT_TX_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ERRCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ERRMSG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SECOND_ERRCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SECOND_ERRMSG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DRAFT_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BIZ_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SYS_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BILL_USE_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TWO_BILL_USE_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TWO_BILL_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TWO_VOUCHER_COUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TWO_IMP_BILL_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  U_TX_INST_ROLE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  U_TX_AGENCY_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  U_TX_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  U_TX_ACCOUNT_SYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  Y_TX_INST_ROLE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  Y_TX_AGENCY_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  Y_TX_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  Y_TX_ACCOUNT_SYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(ID, 0 ) AS ID ,
  COALESCE(MAIN_MSG_ID, 0 ) AS MAIN_MSG_ID ,
  COALESCE(CEN_SERIAL_NO, '' ) AS CEN_SERIAL_NO ,
  COALESCE(CLI_SERIAL_NO, '' ) AS CLI_SERIAL_NO ,
  COALESCE(CLR_DATE, '' ) AS CLR_DATE ,
  COALESCE(BGN_INT_DATE, '' ) AS BGN_INT_DATE ,
  COALESCE(TIME_STAMP, '' ) AS TIME_STAMP ,
  COALESCE(SYS_CODE, '' ) AS SYS_CODE ,
  COALESCE(TX_UNIT_CODE, '' ) AS TX_UNIT_CODE ,
  COALESCE(OPERATOR, '' ) AS OPERATOR ,
  COALESCE(TX_CHARA, '' ) AS TX_CHARA ,
  COALESCE(TX_STATE_CODE, '' ) AS TX_STATE_CODE ,
  COALESCE(OUTSYS_CODE, '' ) AS OUTSYS_CODE ,
  COALESCE(INSYS_CODE, '' ) AS INSYS_CODE ,
  COALESCE(BGN_TX_DATE, '' ) AS BGN_TX_DATE ,
  COALESCE(ORG_JNL_INFO, '' ) AS ORG_JNL_INFO ,
  COALESCE(EXTEND_INFO, '' ) AS EXTEND_INFO ,
  COALESCE(TX_NAMECODE, '' ) AS TX_NAMECODE ,
  COALESCE(O_TX_INST_ROLE, '' ) AS O_TX_INST_ROLE ,
  COALESCE(O_TX_AGENCY_CODE, '' ) AS O_TX_AGENCY_CODE ,
  COALESCE(O_TX_ACCOUNT, '' ) AS O_TX_ACCOUNT ,
  COALESCE(O_TX_ACCOUNT_SYS_CODE, '' ) AS O_TX_ACCOUNT_SYS_CODE ,
  COALESCE(T_TX_INST_ROLE, '' ) AS T_TX_INST_ROLE ,
  COALESCE(T_TX_AGENCY_CODE, '' ) AS T_TX_AGENCY_CODE ,
  COALESCE(T_TX_ACCOUNT, '' ) AS T_TX_ACCOUNT ,
  COALESCE(T_TX_ACCOUNT_SYS_CODE, '' ) AS T_TX_ACCOUNT_SYS_CODE ,
  COALESCE(BILL_TYPE, '' ) AS BILL_TYPE ,
  COALESCE(VOUCHER_COUNT, '' ) AS VOUCHER_COUNT ,
  COALESCE(IMP_BILL_AMT, 0 ) AS IMP_BILL_AMT ,
  COALESCE(A_TX_AMT, 0 ) AS A_TX_AMT ,
  COALESCE(A_AMT_TYPE, '' ) AS A_AMT_TYPE ,
  COALESCE(A_CURR_TYPE, '' ) AS A_CURR_TYPE ,
  COALESCE(A_INT_TX_ACCOUNT, '' ) AS A_INT_TX_ACCOUNT ,
  COALESCE(B_TX_AMT, 0 ) AS B_TX_AMT ,
  COALESCE(B_AMT_TYPE, '' ) AS B_AMT_TYPE ,
  COALESCE(B_CURR_TYPE, '' ) AS B_CURR_TYPE ,
  COALESCE(B_INT_TX_ACCOUNT, '' ) AS B_INT_TX_ACCOUNT ,
  COALESCE(C_TX_AMT, 0 ) AS C_TX_AMT ,
  COALESCE(C_AMT_TYPE, '' ) AS C_AMT_TYPE ,
  COALESCE(C_CURR_TYPE, '' ) AS C_CURR_TYPE ,
  COALESCE(C_INT_TX_ACCOUNT, '' ) AS C_INT_TX_ACCOUNT ,
  COALESCE(D_TX_AMT, 0 ) AS D_TX_AMT ,
  COALESCE(D_AMT_TYPE, '' ) AS D_AMT_TYPE ,
  COALESCE(D_CURR_TYPE, '' ) AS D_CURR_TYPE ,
  COALESCE(D_INT_TX_ACCOUNT, '' ) AS D_INT_TX_ACCOUNT ,
  COALESCE(E_TX_AMT, 0 ) AS E_TX_AMT ,
  COALESCE(E_AMT_TYPE, '' ) AS E_AMT_TYPE ,
  COALESCE(E_CURR_TYPE, '' ) AS E_CURR_TYPE ,
  COALESCE(E_INT_TX_ACCOUNT, '' ) AS E_INT_TX_ACCOUNT ,
  COALESCE(ERRCODE, '' ) AS ERRCODE ,
  COALESCE(ERRMSG, '' ) AS ERRMSG ,
  COALESCE(SECOND_ERRCODE, '' ) AS SECOND_ERRCODE ,
  COALESCE(SECOND_ERRMSG, '' ) AS SECOND_ERRMSG ,
  COALESCE(DRAFT_ID, '' ) AS DRAFT_ID ,
  COALESCE(BIZ_TYPE, '' ) AS BIZ_TYPE ,
  COALESCE(SYS_SERIAL_NO, '' ) AS SYS_SERIAL_NO ,
  COALESCE(BILL_USE_TYPE, '' ) AS BILL_USE_TYPE ,
  COALESCE(TWO_BILL_USE_TYPE, '' ) AS TWO_BILL_USE_TYPE ,
  COALESCE(TWO_BILL_TYPE, '' ) AS TWO_BILL_TYPE ,
  COALESCE(TWO_VOUCHER_COUNT, '' ) AS TWO_VOUCHER_COUNT ,
  COALESCE(TWO_IMP_BILL_AMT, 0 ) AS TWO_IMP_BILL_AMT ,
  COALESCE(U_TX_INST_ROLE, '' ) AS U_TX_INST_ROLE ,
  COALESCE(U_TX_AGENCY_CODE, '' ) AS U_TX_AGENCY_CODE ,
  COALESCE(U_TX_ACCOUNT, '' ) AS U_TX_ACCOUNT ,
  COALESCE(U_TX_ACCOUNT_SYS_CODE, '' ) AS U_TX_ACCOUNT_SYS_CODE ,
  COALESCE(Y_TX_INST_ROLE, '' ) AS Y_TX_INST_ROLE ,
  COALESCE(Y_TX_AGENCY_CODE, '' ) AS Y_TX_AGENCY_CODE ,
  COALESCE(Y_TX_ACCOUNT, '' ) AS Y_TX_ACCOUNT ,
  COALESCE(Y_TX_ACCOUNT_SYS_CODE, '' ) AS Y_TX_ACCOUNT_SYS_CODE ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.BBS_001_SUB_MSG_TOGETHER_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
