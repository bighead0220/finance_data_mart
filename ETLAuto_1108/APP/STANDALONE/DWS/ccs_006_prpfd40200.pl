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
DELETE FROM dw_sdata.CCS_006_PRPFD4 WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCS_006_PRPFD4 SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_137 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCS_006_PRPFD4 WHERE 1=0;

--Step2:
INSERT  INTO T_137 (
  PRD4LNCLS,
  PRD4NAME1,
  PRD4PID,
  PRD4CUR,
  PRD4NAME,
  PRD4TYPE,
  PRD4SUBJ,
  PRD4NAME2,
  PRD4DATE,
  start_dt,
  end_dt)
SELECT
  N.PRD4LNCLS,
  N.PRD4NAME1,
  N.PRD4PID,
  N.PRD4CUR,
  N.PRD4NAME,
  N.PRD4TYPE,
  N.PRD4SUBJ,
  N.PRD4NAME2,
  N.PRD4DATE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(PRD4LNCLS, '' ) AS PRD4LNCLS ,
  COALESCE(PRD4NAME1, '' ) AS PRD4NAME1 ,
  COALESCE(PRD4PID, '' ) AS PRD4PID ,
  COALESCE(PRD4CUR, '' ) AS PRD4CUR ,
  COALESCE(PRD4NAME, '' ) AS PRD4NAME ,
  COALESCE(PRD4TYPE, '' ) AS PRD4TYPE ,
  COALESCE(PRD4SUBJ, '' ) AS PRD4SUBJ ,
  COALESCE(PRD4NAME2, '' ) AS PRD4NAME2 ,
  COALESCE(PRD4DATE, '' ) AS PRD4DATE 
 FROM  dw_tdata.CCS_006_PRPFD4_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  PRD4LNCLS ,
  PRD4NAME1 ,
  PRD4PID ,
  PRD4CUR ,
  PRD4NAME ,
  PRD4TYPE ,
  PRD4SUBJ ,
  PRD4NAME2 ,
  PRD4DATE 
 FROM dw_sdata.CCS_006_PRPFD4 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.PRD4PID = T.PRD4PID AND N.PRD4CUR = T.PRD4CUR AND N.PRD4TYPE = T.PRD4TYPE
WHERE
(T.PRD4PID IS NULL AND T.PRD4CUR IS NULL AND T.PRD4TYPE IS NULL)
 OR N.PRD4LNCLS<>T.PRD4LNCLS
 OR N.PRD4NAME1<>T.PRD4NAME1
 OR N.PRD4NAME<>T.PRD4NAME
 OR N.PRD4SUBJ<>T.PRD4SUBJ
 OR N.PRD4NAME2<>T.PRD4NAME2
 OR N.PRD4DATE<>T.PRD4DATE
;

--Step3:
UPDATE dw_sdata.CCS_006_PRPFD4 P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_137
WHERE P.End_Dt=DATE('2100-12-31')
AND P.PRD4PID=T_137.PRD4PID
AND P.PRD4CUR=T_137.PRD4CUR
AND P.PRD4TYPE=T_137.PRD4TYPE
;

--Step4:
INSERT  INTO dw_sdata.CCS_006_PRPFD4 SELECT * FROM T_137;

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
