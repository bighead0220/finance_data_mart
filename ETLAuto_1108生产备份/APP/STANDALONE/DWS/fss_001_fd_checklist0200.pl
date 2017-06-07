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

DELETE FROM dw_sdata.FSS_001_FD_CHECKLIST WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.FSS_001_FD_CHECKLIST(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  PROVINCECODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  EXTERNDATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  EXTERNSERIAL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BUSINESSTYPECODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AGENTCORPCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  BUSINESSKINDCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ORGANCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OPERCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  LIQUIDATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  ACCTSERIAL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SYSTIME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRADECODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  EXTERNTRADECODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  GREENACCTNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  TRADEAMT1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  AGENTDESC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CHECKSTATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  STOPNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  STOPAMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  STATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  OTHERACCT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  CONSIGNORCODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  SETTLEINST ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  EXPORTFLAG ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(PROVINCECODE, '' ) AS PROVINCECODE ,
  COALESCE(EXTERNDATE, '' ) AS EXTERNDATE ,
  COALESCE(EXTERNSERIAL, 0 ) AS EXTERNSERIAL ,
  COALESCE(BUSINESSTYPECODE, '' ) AS BUSINESSTYPECODE ,
  COALESCE(AGENTCORPCODE, '' ) AS AGENTCORPCODE ,
  COALESCE(BUSINESSKINDCODE, '' ) AS BUSINESSKINDCODE ,
  COALESCE(ORGANCODE, '' ) AS ORGANCODE ,
  COALESCE(OPERCODE, '' ) AS OPERCODE ,
  COALESCE(LIQUIDATE, '' ) AS LIQUIDATE ,
  COALESCE(ACCTSERIAL, 0 ) AS ACCTSERIAL ,
  COALESCE(SYSTIME, '' ) AS SYSTIME ,
  COALESCE(TRADECODE, '' ) AS TRADECODE ,
  COALESCE(EXTERNTRADECODE, '' ) AS EXTERNTRADECODE ,
  COALESCE(GREENACCTNO, '' ) AS GREENACCTNO ,
  COALESCE(TRADEAMT1, 0 ) AS TRADEAMT1 ,
  COALESCE(AGENTDESC, '' ) AS AGENTDESC ,
  COALESCE(CHECKSTATE, '' ) AS CHECKSTATE ,
  COALESCE(STOPNO, '' ) AS STOPNO ,
  COALESCE(STOPAMT, 0 ) AS STOPAMT ,
  COALESCE(STATE, '' ) AS STATE ,
  COALESCE(OTHERACCT, '' ) AS OTHERACCT ,
  COALESCE(CONSIGNORCODE, '' ) AS CONSIGNORCODE ,
  COALESCE(SETTLEINST, '' ) AS SETTLEINST ,
  COALESCE(EXPORTFLAG, '' ) AS EXPORTFLAG ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.FSS_001_FD_CHECKLIST_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
