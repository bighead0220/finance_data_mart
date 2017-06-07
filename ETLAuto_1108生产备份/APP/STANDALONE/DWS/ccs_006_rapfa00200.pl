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
DELETE FROM dw_sdata.CCS_006_RAPFA0 WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CCS_006_RAPFA0 SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_140 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CCS_006_RAPFA0 WHERE 1=0;

--Step2:
INSERT  INTO T_140 (
  RAA0PRE,
  RAA0DPNOK,
  RAA0DPNOA,
  RAA0DATE,
  RAA0FLAG,
  RAA0DUEBNO,
  RAA0LNCLS,
  RAA0TRFROM,
  RAA0DATEB,
  RAA0DATEE,
  RAA0DATEEB,
  RAA0DATEEE,
  RAA0ITRTN,
  RAA0ITRTE,
  RAA0ITRTO,
  RAA0ITROT,
  RAA0OLDYWB,
  RAA0NEWYWB,
  start_dt,
  end_dt)
SELECT
  N.RAA0PRE,
  N.RAA0DPNOK,
  N.RAA0DPNOA,
  N.RAA0DATE,
  N.RAA0FLAG,
  N.RAA0DUEBNO,
  N.RAA0LNCLS,
  N.RAA0TRFROM,
  N.RAA0DATEB,
  N.RAA0DATEE,
  N.RAA0DATEEB,
  N.RAA0DATEEE,
  N.RAA0ITRTN,
  N.RAA0ITRTE,
  N.RAA0ITRTO,
  N.RAA0ITROT,
  N.RAA0OLDYWB,
  N.RAA0NEWYWB,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(RAA0PRE, '' ) AS RAA0PRE ,
  COALESCE(RAA0DPNOK, '' ) AS RAA0DPNOK ,
  COALESCE(RAA0DPNOA, '' ) AS RAA0DPNOA ,
  COALESCE(RAA0DATE, '' ) AS RAA0DATE ,
  COALESCE(RAA0FLAG, '' ) AS RAA0FLAG ,
  COALESCE(RAA0DUEBNO, '' ) AS RAA0DUEBNO ,
  COALESCE(RAA0LNCLS, '' ) AS RAA0LNCLS ,
  COALESCE(RAA0TRFROM, '' ) AS RAA0TRFROM ,
  COALESCE(RAA0DATEB, '' ) AS RAA0DATEB ,
  COALESCE(RAA0DATEE, '' ) AS RAA0DATEE ,
  COALESCE(RAA0DATEEB, '' ) AS RAA0DATEEB ,
  COALESCE(RAA0DATEEE, '' ) AS RAA0DATEEE ,
  COALESCE(RAA0ITRTN, 0 ) AS RAA0ITRTN ,
  COALESCE(RAA0ITRTE, 0 ) AS RAA0ITRTE ,
  COALESCE(RAA0ITRTO, 0 ) AS RAA0ITRTO ,
  COALESCE(RAA0ITROT, 0 ) AS RAA0ITROT ,
  COALESCE(RAA0OLDYWB, '' ) AS RAA0OLDYWB ,
  COALESCE(RAA0NEWYWB, '' ) AS RAA0NEWYWB 
 FROM  dw_tdata.CCS_006_RAPFA0_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  RAA0PRE ,
  RAA0DPNOK ,
  RAA0DPNOA ,
  RAA0DATE ,
  RAA0FLAG ,
  RAA0DUEBNO ,
  RAA0LNCLS ,
  RAA0TRFROM ,
  RAA0DATEB ,
  RAA0DATEE ,
  RAA0DATEEB ,
  RAA0DATEEE ,
  RAA0ITRTN ,
  RAA0ITRTE ,
  RAA0ITRTO ,
  RAA0ITROT ,
  RAA0OLDYWB ,
  RAA0NEWYWB 
 FROM dw_sdata.CCS_006_RAPFA0 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.RAA0DATE = T.RAA0DATE AND N.RAA0DUEBNO = T.RAA0DUEBNO
WHERE
(T.RAA0DATE IS NULL AND T.RAA0DUEBNO IS NULL)
 OR N.RAA0PRE<>T.RAA0PRE
 OR N.RAA0DPNOK<>T.RAA0DPNOK
 OR N.RAA0DPNOA<>T.RAA0DPNOA
 OR N.RAA0FLAG<>T.RAA0FLAG
 OR N.RAA0LNCLS<>T.RAA0LNCLS
 OR N.RAA0TRFROM<>T.RAA0TRFROM
 OR N.RAA0DATEB<>T.RAA0DATEB
 OR N.RAA0DATEE<>T.RAA0DATEE
 OR N.RAA0DATEEB<>T.RAA0DATEEB
 OR N.RAA0DATEEE<>T.RAA0DATEEE
 OR N.RAA0ITRTN<>T.RAA0ITRTN
 OR N.RAA0ITRTE<>T.RAA0ITRTE
 OR N.RAA0ITRTO<>T.RAA0ITRTO
 OR N.RAA0ITROT<>T.RAA0ITROT
 OR N.RAA0OLDYWB<>T.RAA0OLDYWB
 OR N.RAA0NEWYWB<>T.RAA0NEWYWB
;

--Step3:
UPDATE dw_sdata.CCS_006_RAPFA0 P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_140
WHERE P.End_Dt=DATE('2100-12-31')
AND P.RAA0DATE=T_140.RAA0DATE
AND P.RAA0DUEBNO=T_140.RAA0DUEBNO
;

--Step4:
INSERT  INTO dw_sdata.CCS_006_RAPFA0 SELECT * FROM T_140;

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
