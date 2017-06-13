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

DELETE FROM dw_sdata.CCS_006_RAPF80 WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.CCS_006_RAPF80(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  RA80PRE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80DPNOK ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80DPNOA ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80CLITYP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80DUEBNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80CUR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80LNCLS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80TRFROM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80AMTCON ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80OPRS ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80OPR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80STAN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80RECUR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80BJAM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80BNXAMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80BNTAMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80BNFAMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80BWXAMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80BWTAMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80BWFAMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  RA80JSFX ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(RA80PRE, '' ) AS RA80PRE ,
  COALESCE(RA80DATE, '' ) AS RA80DATE ,
  COALESCE(RA80DPNOK, '' ) AS RA80DPNOK ,
  COALESCE(RA80DPNOA, '' ) AS RA80DPNOA ,
  COALESCE(RA80CLITYP, '' ) AS RA80CLITYP ,
  COALESCE(RA80DUEBNO, '' ) AS RA80DUEBNO ,
  COALESCE(RA80CUR, '' ) AS RA80CUR ,
  COALESCE(RA80LNCLS, '' ) AS RA80LNCLS ,
  COALESCE(RA80TRFROM, '' ) AS RA80TRFROM ,
  COALESCE(RA80AMTCON, 0 ) AS RA80AMTCON ,
  COALESCE(RA80OPRS, '' ) AS RA80OPRS ,
  COALESCE(RA80OPR, '' ) AS RA80OPR ,
  COALESCE(RA80STAN, 0 ) AS RA80STAN ,
  COALESCE(RA80RECUR, '' ) AS RA80RECUR ,
  COALESCE(RA80BJAM, 0 ) AS RA80BJAM ,
  COALESCE(RA80BNXAMT, 0 ) AS RA80BNXAMT ,
  COALESCE(RA80BNTAMT, 0 ) AS RA80BNTAMT ,
  COALESCE(RA80BNFAMT, 0 ) AS RA80BNFAMT ,
  COALESCE(RA80BWXAMT, 0 ) AS RA80BWXAMT ,
  COALESCE(RA80BWTAMT, 0 ) AS RA80BWTAMT ,
  COALESCE(RA80BWFAMT, 0 ) AS RA80BWFAMT ,
  COALESCE(RA80JSFX, 0 ) AS RA80JSFX ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.CCS_006_RAPF80_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
