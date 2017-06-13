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

DELETE FROM dw_sdata.CCS_006_JCPF10 WHERE etl_dt=DATE('${TX_DATE_YYYYMMDD}');                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                      
INSERT  INTO dw_sdata.CCS_006_JCPF10(                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             
  JC10PRE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10DATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10GDATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10DPNOK ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10DPNOA ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10TRFROM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10CMSTAN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10STAN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10SYSCOD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10DUEBNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10TYPE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10TRCOD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10CODE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10SEQ ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10CUR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10NOTEXC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10SUBJ ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10ACID ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10NAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10RECALL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10ACNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10ACNOD ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10GACNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10ACTYP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10CRDNO ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10BRIEF ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10ABST ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10DC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10AMT ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10BAL ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10SGN ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10ACCTYP ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10OPRR ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10OPRC ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10OPRA ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10LOCTM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10HOSTM ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10RMK2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10RMK3 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10YWLX ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10COUNT2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10BZ1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10BZ2 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10DATE1 ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10ACDATE ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10STANSQ ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10FNAME ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  JC10FSEQ ,                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
  etl_dt                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              
)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     
SELECT                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                
  COALESCE(JC10PRE, '' ) AS JC10PRE ,
  COALESCE(JC10DATE, '' ) AS JC10DATE ,
  COALESCE(JC10GDATE, '' ) AS JC10GDATE ,
  COALESCE(JC10DPNOK, '' ) AS JC10DPNOK ,
  COALESCE(JC10DPNOA, '' ) AS JC10DPNOA ,
  COALESCE(JC10TRFROM, '' ) AS JC10TRFROM ,
  COALESCE(JC10CMSTAN, 0 ) AS JC10CMSTAN ,
  COALESCE(JC10STAN, 0 ) AS JC10STAN ,
  COALESCE(JC10SYSCOD, 0 ) AS JC10SYSCOD ,
  COALESCE(JC10DUEBNO, '' ) AS JC10DUEBNO ,
  COALESCE(JC10TYPE, '' ) AS JC10TYPE ,
  COALESCE(JC10TRCOD, '' ) AS JC10TRCOD ,
  COALESCE(JC10CODE, '' ) AS JC10CODE ,
  COALESCE(JC10SEQ, 0 ) AS JC10SEQ ,
  COALESCE(JC10CUR, '' ) AS JC10CUR ,
  COALESCE(JC10NOTEXC, '' ) AS JC10NOTEXC ,
  COALESCE(JC10SUBJ, '' ) AS JC10SUBJ ,
  COALESCE(JC10ACID, 0 ) AS JC10ACID ,
  COALESCE(JC10NAME, '' ) AS JC10NAME ,
  COALESCE(JC10RECALL, '' ) AS JC10RECALL ,
  COALESCE(JC10ACNO, '' ) AS JC10ACNO ,
  COALESCE(JC10ACNOD, '' ) AS JC10ACNOD ,
  COALESCE(JC10GACNO, '' ) AS JC10GACNO ,
  COALESCE(JC10ACTYP, '' ) AS JC10ACTYP ,
  COALESCE(JC10CRDNO, '' ) AS JC10CRDNO ,
  COALESCE(JC10BRIEF, '' ) AS JC10BRIEF ,
  COALESCE(JC10ABST, '' ) AS JC10ABST ,
  COALESCE(JC10DC, '' ) AS JC10DC ,
  COALESCE(JC10AMT, 0 ) AS JC10AMT ,
  COALESCE(JC10BAL, 0 ) AS JC10BAL ,
  COALESCE(JC10SGN, '' ) AS JC10SGN ,
  COALESCE(JC10ACCTYP, '' ) AS JC10ACCTYP ,
  COALESCE(JC10OPRR, '' ) AS JC10OPRR ,
  COALESCE(JC10OPRC, '' ) AS JC10OPRC ,
  COALESCE(JC10OPRA, '' ) AS JC10OPRA ,
  COALESCE(JC10LOCTM, '' ) AS JC10LOCTM ,
  COALESCE(JC10HOSTM, '' ) AS JC10HOSTM ,
  COALESCE(JC10RMK2, '' ) AS JC10RMK2 ,
  COALESCE(JC10RMK3, '' ) AS JC10RMK3 ,
  COALESCE(JC10YWLX, '' ) AS JC10YWLX ,
  COALESCE(JC10COUNT2, 0 ) AS JC10COUNT2 ,
  COALESCE(JC10BZ1, '' ) AS JC10BZ1 ,
  COALESCE(JC10BZ2, '' ) AS JC10BZ2 ,
  COALESCE(JC10DATE1, '' ) AS JC10DATE1 ,
  COALESCE(JC10ACDATE, '' ) AS JC10ACDATE ,
  COALESCE(JC10STANSQ, 0 ) AS JC10STANSQ ,
  COALESCE(JC10FNAME, '' ) AS JC10FNAME ,
  COALESCE(JC10FSEQ, '' ) AS JC10FSEQ ,
  DATE('${TX_DATE_YYYYMMDD}')                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
FROM  dw_tdata.CCS_006_JCPF10_${TX_DATE_YYYYMMDD}                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       
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
