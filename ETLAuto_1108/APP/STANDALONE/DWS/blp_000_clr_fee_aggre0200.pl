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

DELETE FROM dw_sdata.BLP_000_CLR_FEE_AGGRE WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.BLP_000_CLR_FEE_AGGRE(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  CLR_FEE_AGGRE_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLR_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DATA_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  MSG_PACK_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SEND_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SEND_TIME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_TIMESTAMP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CEN_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLI_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SYS_SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_SYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OUTSYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INSYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_NAMECODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHAN_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_STATE_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_CHARA ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  UNIT_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  MATCH_ACCT_OPEN_UNIT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_ACCT_OPEN_UNIT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AMT_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CURR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_CURR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_TX_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  HAND_FEE_MODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_TX_ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_TX_ACCT_SYSCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PREF_FEE_UNIT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PREF_FEE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_PROV_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_COLL_UNIT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SHOP_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  MERCHANT_MARK ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TERMI_MARK ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORG_TRAN_INFO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INCOME_PAY_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AGGRE_INST_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ASSG_CYCLE_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SCHE_ASSG_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ASSIGN_STATUS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ASSG_BATCH_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TLR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  LASTDATETIME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(CLR_FEE_AGGRE_ID, 0 ) AS CLR_FEE_AGGRE_ID ,
  COALESCE(CLR_DATE, '' ) AS CLR_DATE ,
  COALESCE(DATA_DATE, '' ) AS DATA_DATE ,
  COALESCE(MSG_PACK_NO, '' ) AS MSG_PACK_NO ,
  COALESCE(SEND_DATE, '' ) AS SEND_DATE ,
  COALESCE(SEND_TIME, '' ) AS SEND_TIME ,
  COALESCE(TX_TIMESTAMP, '' ) AS TX_TIMESTAMP ,
  COALESCE(CEN_SERIAL_NO, '' ) AS CEN_SERIAL_NO ,
  COALESCE(CLI_SERIAL_NO, '' ) AS CLI_SERIAL_NO ,
  COALESCE(SYS_SERIAL_NO, '' ) AS SYS_SERIAL_NO ,
  COALESCE(TX_SYS_CODE, '' ) AS TX_SYS_CODE ,
  COALESCE(OUTSYS_CODE, '' ) AS OUTSYS_CODE ,
  COALESCE(INSYS_CODE, '' ) AS INSYS_CODE ,
  COALESCE(TX_NAMECODE, '' ) AS TX_NAMECODE ,
  COALESCE(CHAN_CODE, '' ) AS CHAN_CODE ,
  COALESCE(TX_STATE_CODE, '' ) AS TX_STATE_CODE ,
  COALESCE(TX_CHARA, '' ) AS TX_CHARA ,
  COALESCE(UNIT_CODE, '' ) AS UNIT_CODE ,
  COALESCE(MATCH_ACCT_OPEN_UNIT, '' ) AS MATCH_ACCT_OPEN_UNIT ,
  COALESCE(TX_ACCT_OPEN_UNIT, '' ) AS TX_ACCT_OPEN_UNIT ,
  COALESCE(AMT_TYPE, '' ) AS AMT_TYPE ,
  COALESCE(CURR_TYPE, '' ) AS CURR_TYPE ,
  COALESCE(TX_AMT, 0 ) AS TX_AMT ,
  COALESCE(FEE_TYPE, '' ) AS FEE_TYPE ,
  COALESCE(FEE_CURR_TYPE, '' ) AS FEE_CURR_TYPE ,
  COALESCE(FEE_TX_AMT, 0 ) AS FEE_TX_AMT ,
  COALESCE(HAND_FEE_MODE, '' ) AS HAND_FEE_MODE ,
  COALESCE(FEE_TX_ACCOUNT, '' ) AS FEE_TX_ACCOUNT ,
  COALESCE(FEE_TX_ACCT_SYSCODE, '' ) AS FEE_TX_ACCT_SYSCODE ,
  COALESCE(PREF_FEE_UNIT, '' ) AS PREF_FEE_UNIT ,
  COALESCE(PREF_FEE, 0 ) AS PREF_FEE ,
  COALESCE(FEE_PROV_FLAG, '' ) AS FEE_PROV_FLAG ,
  COALESCE(FEE_COLL_UNIT, '' ) AS FEE_COLL_UNIT ,
  COALESCE(SHOP_TYPE, '' ) AS SHOP_TYPE ,
  COALESCE(MERCHANT_MARK, '' ) AS MERCHANT_MARK ,
  COALESCE(TERMI_MARK, '' ) AS TERMI_MARK ,
  COALESCE(ORG_TRAN_INFO, '' ) AS ORG_TRAN_INFO ,
  COALESCE(INCOME_PAY_FLAG, '' ) AS INCOME_PAY_FLAG ,
  COALESCE(AGGRE_INST_NO, '' ) AS AGGRE_INST_NO ,
  COALESCE(ASSG_CYCLE_TYPE, '' ) AS ASSG_CYCLE_TYPE ,
  COALESCE(SCHE_ASSG_DATE, '' ) AS SCHE_ASSG_DATE ,
  COALESCE(ASSIGN_STATUS, '' ) AS ASSIGN_STATUS ,
  COALESCE(ASSG_BATCH_NO, '' ) AS ASSG_BATCH_NO ,
  COALESCE(TLR, '' ) AS TLR ,
  COALESCE(LASTDATETIME, '' ) AS LASTDATETIME ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.BLP_000_CLR_FEE_AGGRE_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
