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

DELETE FROM dw_sdata.IBS_001_T_WEB_BANK_JNL WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.IBS_001_T_WEB_BANK_JNL(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  TRAN_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLT_SEQNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  WEB_CLT_SEQNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRAN_TIME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BGN_INT_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRAN_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUB_TRAN_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRAN_ABBR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRAN_INST ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHNL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACC1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACC2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACC3 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACC4 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUB_ACC1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUB_ACC2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUB_ACC3 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SUB_ACC4 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CURR_TYPE1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CURR_TYPE2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CURR_TYPE3 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CURR_TYPE4 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AMT1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AMT2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AMT3 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AMT4 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(TRAN_DATE, 0 ) AS TRAN_DATE ,
  COALESCE(CLT_SEQNO, '' ) AS CLT_SEQNO ,
  COALESCE(WEB_CLT_SEQNO, '' ) AS WEB_CLT_SEQNO ,
  COALESCE(TRAN_TIME, '' ) AS TRAN_TIME ,
  COALESCE(BGN_INT_DATE, 0 ) AS BGN_INT_DATE ,
  COALESCE(TRAN_NO, '' ) AS TRAN_NO ,
  COALESCE(SUB_TRAN_NO, '' ) AS SUB_TRAN_NO ,
  COALESCE(TRAN_ABBR, '' ) AS TRAN_ABBR ,
  COALESCE(TRAN_INST, '' ) AS TRAN_INST ,
  COALESCE(CHNL_NO, '' ) AS CHNL_NO ,
  COALESCE(ACC1, '' ) AS ACC1 ,
  COALESCE(ACC2, '' ) AS ACC2 ,
  COALESCE(ACC3, '' ) AS ACC3 ,
  COALESCE(ACC4, '' ) AS ACC4 ,
  COALESCE(SUB_ACC1, '' ) AS SUB_ACC1 ,
  COALESCE(SUB_ACC2, '' ) AS SUB_ACC2 ,
  COALESCE(SUB_ACC3, '' ) AS SUB_ACC3 ,
  COALESCE(SUB_ACC4, '' ) AS SUB_ACC4 ,
  COALESCE(CURR_TYPE1, '' ) AS CURR_TYPE1 ,
  COALESCE(CURR_TYPE2, '' ) AS CURR_TYPE2 ,
  COALESCE(CURR_TYPE3, '' ) AS CURR_TYPE3 ,
  COALESCE(CURR_TYPE4, '' ) AS CURR_TYPE4 ,
  COALESCE(AMT1, 0 ) AS AMT1 ,
  COALESCE(AMT2, 0 ) AS AMT2 ,
  COALESCE(AMT3, 0 ) AS AMT3 ,
  COALESCE(AMT4, 0 ) AS AMT4 ,
  COALESCE(FLAG, '' ) AS FLAG ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.IBS_001_T_WEB_BANK_JNL_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
