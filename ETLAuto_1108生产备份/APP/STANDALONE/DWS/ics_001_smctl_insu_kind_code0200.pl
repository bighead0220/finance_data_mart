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
DELETE FROM dw_sdata.ICS_001_SMCTL_INSU_KIND_CODE WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ICS_001_SMCTL_INSU_KIND_CODE SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_227 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ICS_001_SMCTL_INSU_KIND_CODE WHERE 1=0;

--Step2:
INSERT  INTO T_227 (
  INSU_CODE,
  INSU_PROD_CODE,
  INSU_KIND_CODE,
  INSU_KIND_NAME,
  INSU_KIND_ENAME,
  INSU_START_DATE,
  INSU_END_DATE,
  ONLINE_FLAG,
  INSU_KIND_FLAG,
  INSU_KIND_TYPE,
  INSU_PAY_MODE,
  PERI_PAY_TYPE,
  PERI_PAY_VALUE,
  ENSU_PERI_TYPE,
  ENSU_PERI_VALUE,
  USE_POL_VCH_CODE,
  start_dt,
  end_dt)
SELECT
  N.INSU_CODE,
  N.INSU_PROD_CODE,
  N.INSU_KIND_CODE,
  N.INSU_KIND_NAME,
  N.INSU_KIND_ENAME,
  N.INSU_START_DATE,
  N.INSU_END_DATE,
  N.ONLINE_FLAG,
  N.INSU_KIND_FLAG,
  N.INSU_KIND_TYPE,
  N.INSU_PAY_MODE,
  N.PERI_PAY_TYPE,
  N.PERI_PAY_VALUE,
  N.ENSU_PERI_TYPE,
  N.ENSU_PERI_VALUE,
  N.USE_POL_VCH_CODE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(INSU_CODE, '' ) AS INSU_CODE ,
  COALESCE(INSU_PROD_CODE, '' ) AS INSU_PROD_CODE ,
  COALESCE(INSU_KIND_CODE, '' ) AS INSU_KIND_CODE ,
  COALESCE(INSU_KIND_NAME, '' ) AS INSU_KIND_NAME ,
  COALESCE(INSU_KIND_ENAME, '' ) AS INSU_KIND_ENAME ,
  COALESCE(INSU_START_DATE, '' ) AS INSU_START_DATE ,
  COALESCE(INSU_END_DATE, '' ) AS INSU_END_DATE ,
  COALESCE(ONLINE_FLAG, '' ) AS ONLINE_FLAG ,
  COALESCE(INSU_KIND_FLAG, '' ) AS INSU_KIND_FLAG ,
  COALESCE(INSU_KIND_TYPE, '' ) AS INSU_KIND_TYPE ,
  COALESCE(INSU_PAY_MODE, '' ) AS INSU_PAY_MODE ,
  COALESCE(PERI_PAY_TYPE, '' ) AS PERI_PAY_TYPE ,
  COALESCE(PERI_PAY_VALUE, 0 ) AS PERI_PAY_VALUE ,
  COALESCE(ENSU_PERI_TYPE, '' ) AS ENSU_PERI_TYPE ,
  COALESCE(ENSU_PERI_VALUE, 0 ) AS ENSU_PERI_VALUE ,
  COALESCE(USE_POL_VCH_CODE, '' ) AS USE_POL_VCH_CODE 
 FROM  dw_tdata.ICS_001_SMCTL_INSU_KIND_CODE_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  INSU_CODE ,
  INSU_PROD_CODE ,
  INSU_KIND_CODE ,
  INSU_KIND_NAME ,
  INSU_KIND_ENAME ,
  INSU_START_DATE ,
  INSU_END_DATE ,
  ONLINE_FLAG ,
  INSU_KIND_FLAG ,
  INSU_KIND_TYPE ,
  INSU_PAY_MODE ,
  PERI_PAY_TYPE ,
  PERI_PAY_VALUE ,
  ENSU_PERI_TYPE ,
  ENSU_PERI_VALUE ,
  USE_POL_VCH_CODE 
 FROM dw_sdata.ICS_001_SMCTL_INSU_KIND_CODE 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.INSU_CODE = T.INSU_CODE AND N.INSU_PROD_CODE = T.INSU_PROD_CODE AND N.INSU_KIND_CODE = T.INSU_KIND_CODE
WHERE
(T.INSU_CODE IS NULL AND T.INSU_PROD_CODE IS NULL AND T.INSU_KIND_CODE IS NULL)
 OR N.INSU_KIND_NAME<>T.INSU_KIND_NAME
 OR N.INSU_KIND_ENAME<>T.INSU_KIND_ENAME
 OR N.INSU_START_DATE<>T.INSU_START_DATE
 OR N.INSU_END_DATE<>T.INSU_END_DATE
 OR N.ONLINE_FLAG<>T.ONLINE_FLAG
 OR N.INSU_KIND_FLAG<>T.INSU_KIND_FLAG
 OR N.INSU_KIND_TYPE<>T.INSU_KIND_TYPE
 OR N.INSU_PAY_MODE<>T.INSU_PAY_MODE
 OR N.PERI_PAY_TYPE<>T.PERI_PAY_TYPE
 OR N.PERI_PAY_VALUE<>T.PERI_PAY_VALUE
 OR N.ENSU_PERI_TYPE<>T.ENSU_PERI_TYPE
 OR N.ENSU_PERI_VALUE<>T.ENSU_PERI_VALUE
 OR N.USE_POL_VCH_CODE<>T.USE_POL_VCH_CODE
;

--Step3:
UPDATE dw_sdata.ICS_001_SMCTL_INSU_KIND_CODE P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_227
WHERE P.End_Dt=DATE('2100-12-31')
AND P.INSU_CODE=T_227.INSU_CODE
AND P.INSU_PROD_CODE=T_227.INSU_PROD_CODE
AND P.INSU_KIND_CODE=T_227.INSU_KIND_CODE
;

--Step4:
INSERT  INTO dw_sdata.ICS_001_SMCTL_INSU_KIND_CODE SELECT * FROM T_227;

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
