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

DELETE FROM dw_sdata.BLP_000_CLR_FEE_DETAIL WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.BLP_000_CLR_FEE_DETAIL(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  CLR_FEE_DETAIL_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLR_FEE_AGGRE_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLR_FEE_ASSG_RESULT_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORG_CLR_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORG_SYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORG_OUTSYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORG_INSYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHAN_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AGGRE_INST_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORG_TRAN_TLR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORG_TRAN_INFO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORG_TERMI_MARK ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INCOME_PAY_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ASSG_INST_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PPAT_ID_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE_CURR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ASSG_AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ASSIGN_STATUS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUM_STATUS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ASSG_BATCH_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ASSIGN_TIMESTAMP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLR_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ASSG_CYCLE_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SCHE_ASSG_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TLR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  LASTDATETIME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(CLR_FEE_DETAIL_ID, 0 ) AS CLR_FEE_DETAIL_ID ,
  COALESCE(CLR_FEE_AGGRE_ID, 0 ) AS CLR_FEE_AGGRE_ID ,
  COALESCE(CLR_FEE_ASSG_RESULT_ID, 0 ) AS CLR_FEE_ASSG_RESULT_ID ,
  COALESCE(ORG_CLR_DATE, '' ) AS ORG_CLR_DATE ,
  COALESCE(ORG_SYS_CODE, '' ) AS ORG_SYS_CODE ,
  COALESCE(ORG_OUTSYS_CODE, '' ) AS ORG_OUTSYS_CODE ,
  COALESCE(ORG_INSYS_CODE, '' ) AS ORG_INSYS_CODE ,
  COALESCE(FEE_TYPE, '' ) AS FEE_TYPE ,
  COALESCE(CHAN_CODE, '' ) AS CHAN_CODE ,
  COALESCE(AGGRE_INST_NO, '' ) AS AGGRE_INST_NO ,
  COALESCE(ORG_TRAN_TLR, '' ) AS ORG_TRAN_TLR ,
  COALESCE(ORG_TRAN_INFO, '' ) AS ORG_TRAN_INFO ,
  COALESCE(ORG_TERMI_MARK, '' ) AS ORG_TERMI_MARK ,
  COALESCE(INCOME_PAY_FLAG, '' ) AS INCOME_PAY_FLAG ,
  COALESCE(ASSG_INST_NO, '' ) AS ASSG_INST_NO ,
  COALESCE(PPAT_ID_TYPE, '' ) AS PPAT_ID_TYPE ,
  COALESCE(FEE_CURR_TYPE, '' ) AS FEE_CURR_TYPE ,
  COALESCE(ASSG_AMT, 0 ) AS ASSG_AMT ,
  COALESCE(ASSIGN_STATUS, '' ) AS ASSIGN_STATUS ,
  COALESCE(SUM_STATUS, '' ) AS SUM_STATUS ,
  COALESCE(ASSG_BATCH_NO, '' ) AS ASSG_BATCH_NO ,
  COALESCE(ASSIGN_TIMESTAMP, '' ) AS ASSIGN_TIMESTAMP ,
  COALESCE(CLR_DATE, '' ) AS CLR_DATE ,
  COALESCE(ASSG_CYCLE_TYPE, '' ) AS ASSG_CYCLE_TYPE ,
  COALESCE(SCHE_ASSG_DATE, '' ) AS SCHE_ASSG_DATE ,
  COALESCE(TLR, '' ) AS TLR ,
  COALESCE(LASTDATETIME, '' ) AS LASTDATETIME ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.BLP_000_CLR_FEE_DETAIL_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
