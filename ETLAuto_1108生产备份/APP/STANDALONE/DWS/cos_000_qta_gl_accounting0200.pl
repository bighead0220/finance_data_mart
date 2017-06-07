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

DELETE FROM dw_sdata.COS_000_QTA_GL_ACCOUNTING WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.COS_000_QTA_GL_ACCOUNTING(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  GL_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BIZ_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACTION_DT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INPUT_TIME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRACK_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  GL_ENTRY_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DEAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRANS_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPERATOR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FLOW_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  GROUP_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DEAL_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CCY ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORGAN_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  COA_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  COA_NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  MAP_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACC_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACC_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AMOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RET_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RET_MSG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  IN_USE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CORRECT_DT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(GL_DATE,DATE('4999-12-31') ) AS GL_DATE ,
  COALESCE(SYS_CODE, '' ) AS SYS_CODE ,
  COALESCE(BIZ_TYPE, '' ) AS BIZ_TYPE ,
  COALESCE(ACTION_DT, '' ) AS ACTION_DT ,
  COALESCE(INPUT_TIME, '' ) AS INPUT_TIME ,
  COALESCE(TRACK_NO, 0 ) AS TRACK_NO ,
  COALESCE(GL_ENTRY_ID, 0 ) AS GL_ENTRY_ID ,
  COALESCE(DEAL_NO, 0 ) AS DEAL_NO ,
  COALESCE(TRANS_TYPE, '' ) AS TRANS_TYPE ,
  COALESCE(OPERATOR, '' ) AS OPERATOR ,
  COALESCE(FLOW_NO, 0 ) AS FLOW_NO ,
  COALESCE(GROUP_NO, '' ) AS GROUP_NO ,
  COALESCE(DEAL_CODE, '' ) AS DEAL_CODE ,
  COALESCE(CCY, '' ) AS CCY ,
  COALESCE(ORGAN_NO, '' ) AS ORGAN_NO ,
  COALESCE(COA_CODE, '' ) AS COA_CODE ,
  COALESCE(COA_NAME, '' ) AS COA_NAME ,
  COALESCE(MAP_CODE, '' ) AS MAP_CODE ,
  COALESCE(ACC_NO, '' ) AS ACC_NO ,
  COALESCE(ACC_TYPE, '' ) AS ACC_TYPE ,
  COALESCE(AMOUNT, 0 ) AS AMOUNT ,
  COALESCE(RET_CODE, '' ) AS RET_CODE ,
  COALESCE(RET_MSG, '' ) AS RET_MSG ,
  COALESCE(IN_USE, '' ) AS IN_USE ,
  COALESCE(CORRECT_DT, '' ) AS CORRECT_DT ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.COS_000_QTA_GL_ACCOUNTING_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
