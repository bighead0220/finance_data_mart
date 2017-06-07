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

DELETE FROM dw_sdata.GTS_000_SPEC_CPDS_FLOW WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.GTS_000_SPEC_CPDS_FLOW(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  EXCH_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLR_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SERIAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SRC_APP_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SRC_TABLE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SRC_EXCH_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TX_NAMECODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DEAL_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACCT_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  EXCH_BAL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FEE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BRANCH_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TELLER_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TIME_STAMP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CD_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  KEY1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  KEY2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  KEY3 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  KEY4 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  MSG_BATCH_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DMMAP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SRC_MSG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUB_SYS_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(ID, '' ) AS ID ,
  COALESCE(EXCH_DATE, '' ) AS EXCH_DATE ,
  COALESCE(CLR_DATE, '' ) AS CLR_DATE ,
  COALESCE(SERIAL_NO, '' ) AS SERIAL_NO ,
  COALESCE(SRC_APP_CODE, '' ) AS SRC_APP_CODE ,
  COALESCE(SRC_TABLE, '' ) AS SRC_TABLE ,
  COALESCE(SRC_EXCH_DATE, '' ) AS SRC_EXCH_DATE ,
  COALESCE(TX_NAMECODE, '' ) AS TX_NAMECODE ,
  COALESCE(DEAL_DATE,DATE('4999-12-31') ) AS DEAL_DATE ,
  COALESCE(ACCT_NO, '' ) AS ACCT_NO ,
  COALESCE(EXCH_BAL, 0 ) AS EXCH_BAL ,
  COALESCE(FEE, 0 ) AS FEE ,
  COALESCE(BRANCH_ID, '' ) AS BRANCH_ID ,
  COALESCE(TELLER_ID, '' ) AS TELLER_ID ,
  COALESCE(TIME_STAMP, '' ) AS TIME_STAMP ,
  COALESCE(CD_FLAG, '' ) AS CD_FLAG ,
  COALESCE(KEY1, '' ) AS KEY1 ,
  COALESCE(KEY2, '' ) AS KEY2 ,
  COALESCE(KEY3, '' ) AS KEY3 ,
  COALESCE(KEY4, '' ) AS KEY4 ,
  COALESCE(MSG_BATCH_NO, '' ) AS MSG_BATCH_NO ,
  COALESCE(DMMAP, '' ) AS DMMAP ,
  COALESCE(SRC_MSG, '' ) AS SRC_MSG ,
  COALESCE(SUB_SYS_CODE, '' ) AS SUB_SYS_CODE ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.GTS_000_SPEC_CPDS_FLOW_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
