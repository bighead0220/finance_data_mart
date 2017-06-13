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
DELETE FROM dw_sdata.CCS_006_RAPF92 WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCS_006_RAPF92 SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_139 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCS_006_RAPF92 WHERE 1=0;

--Step2:
INSERT  INTO T_139 (
  RA92PRE,
  RA92DPNOK,
  RA92DPNOA,
  RA92DUEBNO,
  RA92TRFROM,
  RA92CNT,
  RA92DATEB,
  RA92DATEE,
  RA92BQJXBJ,
  RA92BJAMT,
  RA92DATE,
  RA92BZ,
  start_dt,
  end_dt)
SELECT
  N.RA92PRE,
  N.RA92DPNOK,
  N.RA92DPNOA,
  N.RA92DUEBNO,
  N.RA92TRFROM,
  N.RA92CNT,
  N.RA92DATEB,
  N.RA92DATEE,
  N.RA92BQJXBJ,
  N.RA92BJAMT,
  N.RA92DATE,
  N.RA92BZ,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(RA92PRE, '' ) AS RA92PRE ,
  COALESCE(RA92DPNOK, '' ) AS RA92DPNOK ,
  COALESCE(RA92DPNOA, '' ) AS RA92DPNOA ,
  COALESCE(RA92DUEBNO, '' ) AS RA92DUEBNO ,
  COALESCE(RA92TRFROM, '' ) AS RA92TRFROM ,
  COALESCE(RA92CNT, 0 ) AS RA92CNT ,
  COALESCE(RA92DATEB, '' ) AS RA92DATEB ,
  COALESCE(RA92DATEE, '' ) AS RA92DATEE ,
  COALESCE(RA92BQJXBJ, 0 ) AS RA92BQJXBJ ,
  COALESCE(RA92BJAMT, 0 ) AS RA92BJAMT ,
  COALESCE(RA92DATE, '' ) AS RA92DATE ,
  COALESCE(RA92BZ, '' ) AS RA92BZ 
 FROM  dw_tdata.CCS_006_RAPF92_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  RA92PRE ,
  RA92DPNOK ,
  RA92DPNOA ,
  RA92DUEBNO ,
  RA92TRFROM ,
  RA92CNT ,
  RA92DATEB ,
  RA92DATEE ,
  RA92BQJXBJ ,
  RA92BJAMT ,
  RA92DATE ,
  RA92BZ 
 FROM dw_sdata.CCS_006_RAPF92 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.RA92DUEBNO = T.RA92DUEBNO AND N.RA92CNT = T.RA92CNT
WHERE
(T.RA92DUEBNO IS NULL AND T.RA92CNT IS NULL)
 OR N.RA92PRE<>T.RA92PRE
 OR N.RA92DPNOK<>T.RA92DPNOK
 OR N.RA92DPNOA<>T.RA92DPNOA
 OR N.RA92TRFROM<>T.RA92TRFROM
 OR N.RA92DATEB<>T.RA92DATEB
 OR N.RA92DATEE<>T.RA92DATEE
 OR N.RA92BQJXBJ<>T.RA92BQJXBJ
 OR N.RA92BJAMT<>T.RA92BJAMT
 OR N.RA92DATE<>T.RA92DATE
 OR N.RA92BZ<>T.RA92BZ
;

--Step3:
UPDATE dw_sdata.CCS_006_RAPF92 P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_139
WHERE P.End_Dt=DATE('2100-12-31')
AND P.RA92DUEBNO=T_139.RA92DUEBNO
AND P.RA92CNT=T_139.RA92CNT
;

--Step4:
INSERT  INTO dw_sdata.CCS_006_RAPF92 SELECT * FROM T_139;

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
