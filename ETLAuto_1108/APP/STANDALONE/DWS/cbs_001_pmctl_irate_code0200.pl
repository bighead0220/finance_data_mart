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
DELETE FROM dw_sdata.CBS_001_PMCTL_IRATE_CODE WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.CBS_001_PMCTL_IRATE_CODE SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_80 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.CBS_001_PMCTL_IRATE_CODE WHERE 1=0;

--Step2:
INSERT  INTO T_80 (
  IRATE_CODE,
  IRATE_NAME,
  CUR_CODE,
  IRATE_KIND,
  IRATE_LEVEL,
  IRATE1,
  FLOAT_IRATE,
  IRATE,
  BGN_DATE,
  CHANGE_IRATE_CODE,
  start_dt,
  end_dt)
SELECT
  N.IRATE_CODE,
  N.IRATE_NAME,
  N.CUR_CODE,
  N.IRATE_KIND,
  N.IRATE_LEVEL,
  N.IRATE1,
  N.FLOAT_IRATE,
  N.IRATE,
  N.BGN_DATE,
  N.CHANGE_IRATE_CODE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(IRATE_CODE, '' ) AS IRATE_CODE ,
  COALESCE(IRATE_NAME, '' ) AS IRATE_NAME ,
  COALESCE(CUR_CODE, '' ) AS CUR_CODE ,
  COALESCE(IRATE_KIND, '' ) AS IRATE_KIND ,
  COALESCE(IRATE_LEVEL, '' ) AS IRATE_LEVEL ,
  COALESCE(IRATE1, 0 ) AS IRATE1 ,
  COALESCE(FLOAT_IRATE, 0 ) AS FLOAT_IRATE ,
  COALESCE(IRATE, 0 ) AS IRATE ,
  COALESCE(BGN_DATE, '' ) AS BGN_DATE ,
  COALESCE(CHANGE_IRATE_CODE, '' ) AS CHANGE_IRATE_CODE 
 FROM  dw_tdata.CBS_001_PMCTL_IRATE_CODE_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  IRATE_CODE ,
  IRATE_NAME ,
  CUR_CODE ,
  IRATE_KIND ,
  IRATE_LEVEL ,
  IRATE1 ,
  FLOAT_IRATE ,
  IRATE ,
  BGN_DATE ,
  CHANGE_IRATE_CODE 
 FROM dw_sdata.CBS_001_PMCTL_IRATE_CODE 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.IRATE_CODE = T.IRATE_CODE AND N.CUR_CODE = T.CUR_CODE AND N.BGN_DATE = T.BGN_DATE
WHERE
(T.IRATE_CODE IS NULL AND T.CUR_CODE IS NULL AND T.BGN_DATE IS NULL)
 OR N.IRATE_NAME<>T.IRATE_NAME
 OR N.IRATE_KIND<>T.IRATE_KIND
 OR N.IRATE_LEVEL<>T.IRATE_LEVEL
 OR N.IRATE1<>T.IRATE1
 OR N.FLOAT_IRATE<>T.FLOAT_IRATE
 OR N.IRATE<>T.IRATE
 OR N.CHANGE_IRATE_CODE<>T.CHANGE_IRATE_CODE
;

--Step3:
UPDATE dw_sdata.CBS_001_PMCTL_IRATE_CODE P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_80
WHERE P.End_Dt=DATE('2100-12-31')
AND P.IRATE_CODE=T_80.IRATE_CODE
AND P.CUR_CODE=T_80.CUR_CODE
AND P.BGN_DATE=T_80.BGN_DATE
;

--Step4:
INSERT  INTO dw_sdata.CBS_001_PMCTL_IRATE_CODE SELECT * FROM T_80;

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
