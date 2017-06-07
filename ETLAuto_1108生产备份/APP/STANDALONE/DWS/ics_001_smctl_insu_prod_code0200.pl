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
DELETE FROM dw_sdata.ICS_001_SMCTL_INSU_PROD_CODE WHERE start_dt>=DATE('${TX_DATE_YYYYMMDD}');
UPDATE dw_sdata.ICS_001_SMCTL_INSU_PROD_CODE SET end_dt=DATE('2100-12-31') WHERE end_dt>=DATE('${TX_DATE_YYYYMMDD}') AND end_dt<>DATE('2100-12-31');

--Step1:
CREATE LOCAL TEMPORARY TABLE  T_228 ON COMMIT PRESERVE ROWS AS SELECT * FROM dw_sdata.ICS_001_SMCTL_INSU_PROD_CODE WHERE 1=0;

--Step2:
INSERT  INTO T_228 (
  INSU_CODE,
  INSU_PROD_CODE,
  INSU_PROD_TYPE,
  INSU_PROD_NAME,
  LS_INSU_FLAG,
  LIFE_INSU_TYPE,
  PROP_INSU_TYPE,
  NON_INV_PROP_TYPE,
  CAR_INSU_TYPE,
  start_dt,
  end_dt)
SELECT
  N.INSU_CODE,
  N.INSU_PROD_CODE,
  N.INSU_PROD_TYPE,
  N.INSU_PROD_NAME,
  N.LS_INSU_FLAG,
  N.LIFE_INSU_TYPE,
  N.PROP_INSU_TYPE,
  N.NON_INV_PROP_TYPE,
  N.CAR_INSU_TYPE,
  DATE('${TX_DATE_YYYYMMDD}'),
  DATE('2100-12-31')
FROM 
 (SELECT
  COALESCE(INSU_CODE, '' ) AS INSU_CODE ,
  COALESCE(INSU_PROD_CODE, '' ) AS INSU_PROD_CODE ,
  COALESCE(INSU_PROD_TYPE, '' ) AS INSU_PROD_TYPE ,
  COALESCE(INSU_PROD_NAME, '' ) AS INSU_PROD_NAME ,
  COALESCE(LS_INSU_FLAG, '' ) AS LS_INSU_FLAG ,
  COALESCE(LIFE_INSU_TYPE, '' ) AS LIFE_INSU_TYPE ,
  COALESCE(PROP_INSU_TYPE, '' ) AS PROP_INSU_TYPE ,
  COALESCE(NON_INV_PROP_TYPE, '' ) AS NON_INV_PROP_TYPE ,
  COALESCE(CAR_INSU_TYPE, '' ) AS CAR_INSU_TYPE 
 FROM  dw_tdata.ICS_001_SMCTL_INSU_PROD_CODE_${TX_DATE_YYYYMMDD}) N
LEFT JOIN
 (SELECT 
  INSU_CODE ,
  INSU_PROD_CODE ,
  INSU_PROD_TYPE ,
  INSU_PROD_NAME ,
  LS_INSU_FLAG ,
  LIFE_INSU_TYPE ,
  PROP_INSU_TYPE ,
  NON_INV_PROP_TYPE ,
  CAR_INSU_TYPE 
 FROM dw_sdata.ICS_001_SMCTL_INSU_PROD_CODE 
 WHERE END_DT = DATE('2100-12-31') ) T
ON N.INSU_CODE = T.INSU_CODE AND N.INSU_PROD_CODE = T.INSU_PROD_CODE
WHERE
(T.INSU_CODE IS NULL AND T.INSU_PROD_CODE IS NULL)
 OR N.INSU_PROD_TYPE<>T.INSU_PROD_TYPE
 OR N.INSU_PROD_NAME<>T.INSU_PROD_NAME
 OR N.LS_INSU_FLAG<>T.LS_INSU_FLAG
 OR N.LIFE_INSU_TYPE<>T.LIFE_INSU_TYPE
 OR N.PROP_INSU_TYPE<>T.PROP_INSU_TYPE
 OR N.NON_INV_PROP_TYPE<>T.NON_INV_PROP_TYPE
 OR N.CAR_INSU_TYPE<>T.CAR_INSU_TYPE
;

--Step3:
UPDATE dw_sdata.ICS_001_SMCTL_INSU_PROD_CODE P 
SET End_Dt=DATE('${TX_DATE_YYYYMMDD}')
FROM T_228
WHERE P.End_Dt=DATE('2100-12-31')
AND P.INSU_CODE=T_228.INSU_CODE
AND P.INSU_PROD_CODE=T_228.INSU_PROD_CODE
;

--Step4:
INSERT  INTO dw_sdata.ICS_001_SMCTL_INSU_PROD_CODE SELECT * FROM T_228;

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
