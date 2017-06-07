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

--Step0:
DELETE FROM dw_sdata.ISS_001_CR_LCINFOSUCC WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ISS_001_CR_LCINFOSUCC SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_231 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ISS_001_CR_LCINFOSUCC WHERE 1=0;

--Step2:
INSERT  INTO T_231 (
  MSGNO,
  LCCUR,
  LCAMT,
  TOLER,
  MAXAMT,
  DAYS,
  NOTES,
  RATIO,
  DEPCUR,
  DEPAMT,
  DEPACCTNO,
  INDATE,
  CURRENTSTATUS,
  LCNO,
  PAYMENGTYPE,
  AMENDSTATUS,
  start_dt,
  end_dt)
SELECT
  N.MSGNO,
  N.LCCUR,
  N.LCAMT,
  N.TOLER,
  N.MAXAMT,
  N.DAYS,
  N.NOTES,
  N.RATIO,
  N.DEPCUR,
  N.DEPAMT,
  N.DEPACCTNO,
  N.INDATE,
  N.CURRENTSTATUS,
  N.LCNO,
  N.PAYMENGTYPE,
  N.AMENDSTATUS,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(MSGNO, '' ) AS MSGNO ,
  COALESCE(LCCUR, '' ) AS LCCUR ,
  COALESCE(LCAMT, 0 ) AS LCAMT ,
  COALESCE(TOLER, '' ) AS TOLER ,
  COALESCE(MAXAMT, 0 ) AS MAXAMT ,
  COALESCE(DAYS, '' ) AS DAYS ,
  COALESCE(NOTES, 0 ) AS NOTES ,
  COALESCE(RATIO, '' ) AS RATIO ,
  COALESCE(DEPCUR, '' ) AS DEPCUR ,
  COALESCE(DEPAMT, '' ) AS DEPAMT ,
  COALESCE(DEPACCTNO, '' ) AS DEPACCTNO ,
  COALESCE(INDATE,DATE('4999-12-31') ) AS INDATE ,
  COALESCE(CURRENTSTATUS, '' ) AS CURRENTSTATUS ,
  COALESCE(LCNO, '' ) AS LCNO ,
  COALESCE(PAYMENGTYPE, '' ) AS PAYMENGTYPE ,
  COALESCE(AMENDSTATUS, '' ) AS AMENDSTATUS 
 FROM  dw_tdata.ISS_001_CR_LCINFOSUCC_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  MSGNO ,
  LCCUR ,
  LCAMT ,
  TOLER ,
  MAXAMT ,
  DAYS ,
  NOTES ,
  RATIO ,
  DEPCUR ,
  DEPAMT ,
  DEPACCTNO ,
  INDATE ,
  CURRENTSTATUS ,
  LCNO ,
  PAYMENGTYPE ,
  AMENDSTATUS 
 FROM dw_sdata.ISS_001_CR_LCINFOSUCC 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.MSGNO = T.MSGNO AND N.INDATE = T.INDATE
WHERE
(T.MSGNO IS NULL AND T.INDATE IS NULL)
 OR N.LCCUR<>T.LCCUR
 OR N.LCAMT<>T.LCAMT
 OR N.TOLER<>T.TOLER
 OR N.MAXAMT<>T.MAXAMT
 OR N.DAYS<>T.DAYS
 OR N.NOTES<>T.NOTES
 OR N.RATIO<>T.RATIO
 OR N.DEPCUR<>T.DEPCUR
 OR N.DEPAMT<>T.DEPAMT
 OR N.DEPACCTNO<>T.DEPACCTNO
 OR N.CURRENTSTATUS<>T.CURRENTSTATUS
 OR N.LCNO<>T.LCNO
 OR N.PAYMENGTYPE<>T.PAYMENGTYPE
 OR N.AMENDSTATUS<>T.AMENDSTATUS
;

--Step3:
UPDATE dw_sdata.ISS_001_CR_LCINFOSUCC P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_231
WHERE P.End_Dt=DATE('2100-12-31')
AND P.MSGNO=T_231.MSGNO
AND P.INDATE=T_231.INDATE
;

--Step4:
INSERT  INTO dw_sdata.ISS_001_CR_LCINFOSUCC SELECT * FROM T_231;

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
