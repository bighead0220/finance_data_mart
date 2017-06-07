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

DELETE FROM dw_sdata.ACC_004_T_ACCDATA_MAIN WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.ACC_004_T_ACCDATA_MAIN(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  SYSTEM_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  HOST_SEQNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CLT_SEQNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACC_DATA_ID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACC_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BGN_INT_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TALLY_DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRAN_TIME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CURR_TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ENTRY_SERNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRAN_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ENTRY_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ENTRY_NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ENTRY_ABBR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACC_FLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TLR_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRAN_INST ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACC_CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  GLOBAL_NO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SYSTEM_SEQNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(SYSTEM_ID, '' ) AS SYSTEM_ID ,
  COALESCE(HOST_SEQNO, '' ) AS HOST_SEQNO ,
  COALESCE(CLT_SEQNO, '' ) AS CLT_SEQNO ,
  COALESCE(ACC_DATA_ID, '' ) AS ACC_DATA_ID ,
  COALESCE(ACC_DATE, '' ) AS ACC_DATE ,
  COALESCE(BGN_INT_DATE, '' ) AS BGN_INT_DATE ,
  COALESCE(TALLY_DATE, '' ) AS TALLY_DATE ,
  COALESCE(TRAN_TIME, '' ) AS TRAN_TIME ,
  COALESCE(CURR_TYPE, '' ) AS CURR_TYPE ,
  COALESCE(ENTRY_SERNO, 0 ) AS ENTRY_SERNO ,
  COALESCE(TRAN_CODE, '' ) AS TRAN_CODE ,
  COALESCE(ENTRY_CODE, '' ) AS ENTRY_CODE ,
  COALESCE(ENTRY_NAME, '' ) AS ENTRY_NAME ,
  COALESCE(ENTRY_ABBR, '' ) AS ENTRY_ABBR ,
  COALESCE(ACC_FLAG, 0 ) AS ACC_FLAG ,
  COALESCE(TLR_NO, '' ) AS TLR_NO ,
  COALESCE(TRAN_INST, '' ) AS TRAN_INST ,
  COALESCE(ACC_CODE, '' ) AS ACC_CODE ,
  COALESCE(GLOBAL_NO, '' ) AS GLOBAL_NO ,
  COALESCE(SYSTEM_SEQNO, '' ) AS SYSTEM_SEQNO ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.ACC_004_T_ACCDATA_MAIN_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
