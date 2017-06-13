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

DELETE FROM dw_sdata.CBS_001_AMDTL_PREP_INT WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.CBS_001_AMDTL_PREP_INT(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  ACCOUNT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUBACCT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DRAW_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CUR_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DP_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  REV_PAY_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DEP_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INT_START_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  INTER_DAYS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BAL_WEIT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CUR_COPE_INT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  LST_INT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CUR_FACE_INT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUB_SYS_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  DRAW_INT_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  PREP_INT_TYP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BALANCE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACC_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(ACCOUNT, '' ) AS ACCOUNT ,
  COALESCE(SUBACCT, 0 ) AS SUBACCT ,
  COALESCE(DRAW_DATE, '' ) AS DRAW_DATE ,
  COALESCE(CUR_CODE, '' ) AS CUR_CODE ,
  COALESCE(DP_NO, 0 ) AS DP_NO ,
  COALESCE(REV_PAY_FLAG, '' ) AS REV_PAY_FLAG ,
  COALESCE(DEP_TYPE, '' ) AS DEP_TYPE ,
  COALESCE(INT_START_DATE, '' ) AS INT_START_DATE ,
  COALESCE(INTER_DAYS, 0 ) AS INTER_DAYS ,
  COALESCE(BAL_WEIT, 0 ) AS BAL_WEIT ,
  COALESCE(RATE, 0 ) AS RATE ,
  COALESCE(CUR_COPE_INT, 0 ) AS CUR_COPE_INT ,
  COALESCE(LST_INT, 0 ) AS LST_INT ,
  COALESCE(CUR_FACE_INT, 0 ) AS CUR_FACE_INT ,
  COALESCE(SUB_SYS_FLAG, '' ) AS SUB_SYS_FLAG ,
  COALESCE(DRAW_INT_FLAG, '' ) AS DRAW_INT_FLAG ,
  COALESCE(PREP_INT_TYP, '' ) AS PREP_INT_TYP ,
  COALESCE(BALANCE, 0 ) AS BALANCE ,
  COALESCE(ACC_FLAG, '' ) AS ACC_FLAG ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.CBS_001_AMDTL_PREP_INT_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
