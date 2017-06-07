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

DELETE FROM dw_sdata.CCS_006_JAPF10 WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.CCS_006_JAPF10(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  JA10PRE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10GDATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10DPNOK ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10DPNOA ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10TRFROM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10CMSTAN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10STAN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10SYSCOD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10DUEBNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10TRCOD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10SEQ ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10CUR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10NOTEXC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10SUBJ ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10ACID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10RECALL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10ACNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10ACNOD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10GACNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10ACTYP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10CRDNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10BRIEF ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10ABST ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10DC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10BAL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10SGN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10ACCTYP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10OPRR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10OPRC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10OPRA ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10LOCTM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10HOSTM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10RMK2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10RMK3 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10YWLX ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10COUNT2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10BZ1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10BZ2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10DATE1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10ACDATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10STANSQ ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10FNAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JA10FSEQ ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(JA10PRE, '' ) AS JA10PRE ,
  COALESCE(JA10DATE, '' ) AS JA10DATE ,
  COALESCE(JA10GDATE, '' ) AS JA10GDATE ,
  COALESCE(JA10DPNOK, '' ) AS JA10DPNOK ,
  COALESCE(JA10DPNOA, '' ) AS JA10DPNOA ,
  COALESCE(JA10TRFROM, '' ) AS JA10TRFROM ,
  COALESCE(JA10CMSTAN, 0 ) AS JA10CMSTAN ,
  COALESCE(JA10STAN, 0 ) AS JA10STAN ,
  COALESCE(JA10SYSCOD, 0 ) AS JA10SYSCOD ,
  COALESCE(JA10DUEBNO, '' ) AS JA10DUEBNO ,
  COALESCE(JA10TYPE, '' ) AS JA10TYPE ,
  COALESCE(JA10TRCOD, '' ) AS JA10TRCOD ,
  COALESCE(JA10CODE, '' ) AS JA10CODE ,
  COALESCE(JA10SEQ, 0 ) AS JA10SEQ ,
  COALESCE(JA10CUR, '' ) AS JA10CUR ,
  COALESCE(JA10NOTEXC, '' ) AS JA10NOTEXC ,
  COALESCE(JA10SUBJ, '' ) AS JA10SUBJ ,
  COALESCE(JA10ACID, 0 ) AS JA10ACID ,
  COALESCE(JA10NAME, '' ) AS JA10NAME ,
  COALESCE(JA10RECALL, '' ) AS JA10RECALL ,
  COALESCE(JA10ACNO, '' ) AS JA10ACNO ,
  COALESCE(JA10ACNOD, '' ) AS JA10ACNOD ,
  COALESCE(JA10GACNO, '' ) AS JA10GACNO ,
  COALESCE(JA10ACTYP, '' ) AS JA10ACTYP ,
  COALESCE(JA10CRDNO, '' ) AS JA10CRDNO ,
  COALESCE(JA10BRIEF, '' ) AS JA10BRIEF ,
  COALESCE(JA10ABST, '' ) AS JA10ABST ,
  COALESCE(JA10DC, '' ) AS JA10DC ,
  COALESCE(JA10AMT, 0 ) AS JA10AMT ,
  COALESCE(JA10BAL, 0 ) AS JA10BAL ,
  COALESCE(JA10SGN, '' ) AS JA10SGN ,
  COALESCE(JA10ACCTYP, '' ) AS JA10ACCTYP ,
  COALESCE(JA10OPRR, '' ) AS JA10OPRR ,
  COALESCE(JA10OPRC, '' ) AS JA10OPRC ,
  COALESCE(JA10OPRA, '' ) AS JA10OPRA ,
  COALESCE(JA10LOCTM, '' ) AS JA10LOCTM ,
  COALESCE(JA10HOSTM, '' ) AS JA10HOSTM ,
  COALESCE(JA10RMK2, '' ) AS JA10RMK2 ,
  COALESCE(JA10RMK3, '' ) AS JA10RMK3 ,
  COALESCE(JA10YWLX, '' ) AS JA10YWLX ,
  COALESCE(JA10COUNT2, 0 ) AS JA10COUNT2 ,
  COALESCE(JA10BZ1, '' ) AS JA10BZ1 ,
  COALESCE(JA10BZ2, '' ) AS JA10BZ2 ,
  COALESCE(JA10DATE1, '' ) AS JA10DATE1 ,
  COALESCE(JA10ACDATE, '' ) AS JA10ACDATE ,
  COALESCE(JA10STANSQ, 0 ) AS JA10STANSQ ,
  COALESCE(JA10FNAME, '' ) AS JA10FNAME ,
  COALESCE(JA10FSEQ, '' ) AS JA10FSEQ ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.CCS_006_JAPF10_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
